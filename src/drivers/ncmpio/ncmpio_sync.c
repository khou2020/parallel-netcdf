/*
 *  Copyright (C) 2003, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

/*
 * This file implements the corresponding APIs defined in src/dispatchers/file.c
 *
 * ncmpi_sync()             : dispatcher->sync()
 * ncmpi_sync_numrecs()     : dispatcher->sync_numrecs()
 */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <mpi.h>

#include <pnc_debug.h>
#include <common.h>
#include "nc.h"
#include "ncx.h"

/*----< ncmpiio_sync() >-----------------------------------------------------*/
/* This function must be called collectively, no matter if it is in collective
 * or independent data mode.
 */
int
ncmpiio_sync(NC *ncp) {
#ifndef DISABLE_FILE_SYNC
    int mpireturn;

    if (ncp->independent_fh != MPI_FILE_NULL) {
        TRACE_IO(MPI_File_sync)(ncp->independent_fh);
        if (mpireturn != MPI_SUCCESS)
            return ncmpii_handle_error(mpireturn, "MPI_File_sync");
    }

    /* ncp->collective_fh is never MPI_FILE_NULL as collective mode is
     * default in PnetCDF */
    TRACE_IO(MPI_File_sync)(ncp->collective_fh);
    if (mpireturn != MPI_SUCCESS)
        return ncmpii_handle_error(mpireturn, "MPI_File_sync");

    TRACE_COMM(MPI_Barrier)(ncp->comm);
#endif
    return NC_NOERR;
}

#define NC_NUMRECS_OFFSET 4

/*----< ncmpii_write_numrecs() >---------------------------------------------*/
/* root process writes the new record number into file.
 * This function is called by:
 * 1. ncmpiio_sync_numrecs
 * 2. collective nonblocking wait API, if the new number of records is bigger
 */
int
ncmpii_write_numrecs(NC         *ncp,
                     MPI_Offset  new_numrecs)
{
    int rank, mpireturn, err;
    MPI_File fh;

    /* root process writes numrecs in file */
    MPI_Comm_rank(ncp->comm, &rank);
    if (rank > 0) return NC_NOERR;

    /* return now if there is no record variabled defined */
    if (ncp->vars.num_rec_vars == 0) return NC_NOERR;

    fh = ncp->collective_fh;
    if (NC_indep(ncp))
        fh = ncp->independent_fh;

    if (new_numrecs > ncp->numrecs || NC_ndirty(ncp)) {
        int len;
        char pos[8], *buf=pos;
        MPI_Offset max_numrecs;
        MPI_Status mpistatus;

        max_numrecs = MAX(new_numrecs, ncp->numrecs);

        if (ncp->format < 5) {
            if (max_numrecs != (int)max_numrecs)
                DEBUG_RETURN_ERROR(NC_EINTOVERFLOW)
            len = X_SIZEOF_SIZE_T;
            err = ncmpix_put_uint32((void**)&buf, (uint)max_numrecs);
            if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)
        }
        else {
            len = X_SIZEOF_INT64;
            err = ncmpix_put_uint64((void**)&buf, (uint64)max_numrecs);
            if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)
        }
        /* ncmpix_put_xxx advances the 1st argument with size len */

        /* root's file view always includes the entire file header */
        TRACE_IO(MPI_File_write_at)(fh, NC_NUMRECS_OFFSET, (void*)pos, len,
                                    MPI_BYTE, &mpistatus);
        if (mpireturn != MPI_SUCCESS) {
            err = ncmpii_handle_error(mpireturn, "MPI_File_write_at");
            if (err == NC_EFILE) DEBUG_RETURN_ERROR(NC_EWRITE)
        }
        else {
            ncp->put_size += len;
        }
    }
    return NC_NOERR;
}

/*----< ncmpiio_sync_numrecs() >----------------------------------------------*/
/* Synchronize the number of records in memory and write numrecs to file.
 * This function is called by:
 * 1. ncmpi_sync_numrecs(): by the user
 * 2. ncmpi_sync(): by the user
 * 3. ncmpii_end_indep_data(): exit from independent data mode
 * 4. all blocking collective put APIs when writing record variables
 * 5. ncmpii_close(): file close and currently in independent data mode
 *
 * This function is collective.
 */
int
ncmpiio_sync_numrecs(NC         *ncp,
                     MPI_Offset  new_numrecs)
{
    int status=NC_NOERR, mpireturn;
    MPI_Offset max_numrecs;

    assert(!NC_readonly(ncp));
    assert(!NC_indef(ncp)); /* can only be called by APIs in data mode */

    /* return now if there is no record variabled defined */
    if (ncp->vars.num_rec_vars == 0) return NC_NOERR;

    /* find the max new_numrecs among all processes
     * Note new_numrecs may be smaller than ncp->numrecs
     */
    TRACE_COMM(MPI_Allreduce)(&new_numrecs, &max_numrecs, 1, MPI_OFFSET,
                              MPI_MAX, ncp->comm);
    if (mpireturn != MPI_SUCCESS)
        return ncmpii_handle_error(mpireturn, "MPI_Allreduce");

    /* root process writes max_numrecs to file */
    status = ncmpii_write_numrecs(ncp, max_numrecs);

    if (ncp->safe_mode == 1) {
        /* broadcast root's status, because only root writes to the file */
        int root_status = status;
        TRACE_COMM(MPI_Bcast)(&root_status, 1, MPI_INT, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS)
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        /* root's write has failed, which is serious */
        if (root_status == NC_EWRITE) DEBUG_ASSIGN_ERROR(status, NC_EWRITE)
    }

    /* update numrecs in all processes's memory only if the new one is larger.
     * Note new_numrecs may be smaller than ncp->numrecs
     */
    if (max_numrecs > ncp->numrecs) ncp->numrecs = max_numrecs;

    /* clear numrecs dirty bit */
    fClr(ncp->flags, NC_NDIRTY);

    return status;
}

/*----< ncmpii_sync_numrecs() >-----------------------------------------------*/
/* this API is collective, but can be called in independent data mode.
 * Note numrecs is always sync-ed in memory and update in file in collective
 * data mode.
 */
int
ncmpii_sync_numrecs(void *ncdp)
{
    int err=NC_NOERR;
    NC *ncp=(NC*)ncdp;

    /* cannot be in define mode */
    if (NC_indef(ncp)) DEBUG_RETURN_ERROR(NC_EINDEFINE)

    /* check if we have defined record variables */
    if (ncp->vars.num_rec_vars == 0) return NC_NOERR;

    if (!NC_indep(ncp)) /* in collective data mode, numrecs is always sync-ed */
        return NC_NOERR;
    else /* if called in independent mode, we force sync in memory */
        set_NC_ndirty(ncp);

    /* sync numrecs in memory and file */
    err = ncmpiio_sync_numrecs(ncp, ncp->numrecs);

#ifndef DISABLE_FILE_SYNC
    if (NC_doFsync(ncp)) { /* NC_SHARE is set */
        int mpierr, mpireturn;
        if (NC_indep(ncp)) {
            TRACE_IO(MPI_File_sync)(ncp->independent_fh);
        }
        else {
            TRACE_IO(MPI_File_sync)(ncp->collective_fh);
        }
        if (mpireturn != MPI_SUCCESS) {
            mpierr = ncmpii_handle_error(mpireturn, "MPI_File_sync");
            if (err == NC_NOERR) err = mpierr;
        }
        TRACE_COMM(MPI_Barrier)(ncp->comm);
        if (mpireturn != MPI_SUCCESS)
            return ncmpii_handle_error(mpireturn, "MPI_Barrier");
    }
#endif
    return err;
}

/*----< ncmpii_sync() >------------------------------------------------------*/
/* This API is a collective subroutine, and must be called in data mode, no
 * matter if it is in collective or independent data mode.
 */
int
ncmpii_sync(void *ncdp)
{
    int err, status = NC_NOERR;
    NC *ncp = (NC*)ncdp;

    /* cannot be in define mode */
    if (NC_indef(ncp)) DEBUG_RETURN_ERROR(NC_EINDEFINE)

    if (NC_readonly(ncp))
        /* calling sync for file opened for read only means re-read header */
        return ncmpii_read_NC(ncp);

    /* the only part of header that can be dirty is numrecs (caused only by
     * independent APIs) */
    if (ncp->vars.num_rec_vars > 0 && NC_indep(ncp)) {
        /* sync numrecs in memory among processes and in file */
        set_NC_ndirty(ncp);
        err = ncmpiio_sync_numrecs(ncp, ncp->numrecs);
        if (err != NC_NOERR) return err;
    }

    /* Flush the log if flushing flag is set */
    if (ncp->nclogp != NULL){
        if (ncp->loghints & NC_LOG_HINT_FLUSH_ON_SYNC){
            /* Prevent recursive flushing if wait is called by log_flush */
            if (ncp->nclogp->isflushing == NC_LOG_FALSE){
                err = ncmpii_log_flush(ncp);       
                if (status == NC_NOERR){
                    status = err;                      
                }
            }
        }
    }


    /* calling MPI_File_sync() on both collective and independent handlers */
    err = ncmpiio_sync(ncp);
    if (status == NC_NOERR){
        status = err;                      
    }

    return status;
}

