/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

/*
 * This file implements the following PnetCDF APIs
 *
 * ncmpi_create()           : dispatcher->create()
 * ncmpi_open()             : dispatcher->open()
 * ncmpi_close()            : dispatcher->close()
 * ncmpi_enddef()           : dispatcher->enddef()
 * ncmpi__enddef()          : dispatcher->_enddef()
 * ncmpi_redef()            : dispatcher->redef()
 * ncmpi_begin_indep_data() : dispatcher->begin_indep_data()
 * ncmpi_end_indep_data()   : dispatcher->end_indep_data()
 * ncmpi_abort()            : dispatcher->abort()
 * ncmpi_inq()              : dispatcher->inq()
 * ncmpi_inq_misc()         : dispatcher->inq_misc()
 * ncmpi_wait()             : dispatcher->wait()
 * ncmpi_wait_all()         : dispatcher->wait()
 * ncmpi_cancel()           : dispatcher->cancel()
 *
 * ncmpi_set_fill()         : dispatcher->set_fill()
 * ncmpi_fill_var_rec()     : dispatcher->fill_rec()
 * ncmpi_def_var_fill()     : dispatcher->def_var_fill()
 * ncmpi_inq_var_fill()     : dispatcher->inq()
 *
 * ncmpi_sync()             : dispatcher->sync()
 * ncmpi_sync_numrecs()     : dispatcher->sync_numrecs()
 *
 */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h> /* strlen() */

#include <mpi.h>
#include <pnc_debug.h>
#include <common.h>
#include <ncbbio_driver.h>

int
ncbbio_create(MPI_Comm     comm,
             const char  *path,
             int          cmode,
             int          ncid,
             MPI_Info     info,
             void       **ncpp)  /* OUT */
{
    int err;
    void *ncp=NULL;
    NC_bb *ncbbp;
    PNC_driver *driver=NULL;

    /* TODO: use comde to determine the true driver */
    driver = ncmpio_inq_driver();
    if (driver == NULL) return NC_ENOTNC;

    err = driver->create(comm, path, cmode, ncid, info, &ncp);
    if (err != NC_NOERR) return err;

    /* Create a NC_bb object and save its driver pointer */
    ncbbp = (NC_bb*) NCI_Malloc(sizeof(NC_bb));
    if (ncbbp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
   
    ncbbp->path = (char*) NCI_Malloc(strlen(path)+1);
    if (ncbbp->path == NULL) {
        NCI_Free(ncbbp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(ncbbp->path, path);
    ncbbp->mode       = cmode;
    ncbbp->ncmpio_driver     = driver;
    ncbbp->flag       = 0;
    ncbbp->ncid = ncid;
    ncbbp->curreqid = -1;
    ncbbp->ncp        = ncp;
    MPI_Comm_dup(comm, &ncbbp->comm);
    
    /* Init log structure */
    err = ncmpii_log_create(ncbbp, info);
    if (err != NC_NOERR) {
        NCI_Free(ncbbp);
        return err;
    }
    ncbbp->inited = 1;
    
    *ncpp = ncbbp;

    return NC_NOERR;
}

int
ncbbio_open(MPI_Comm     comm,
           const char  *path,
           int          omode,
           int          ncid,
           MPI_Info     info,
           void       **ncpp)
{
    int err, format, flag;
    void *ncp=NULL;
    NC_bb *ncbbp;
    PNC_driver *driver=NULL;
    char value[MPI_MAX_INFO_VAL];
    
    err = ncmpi_inq_file_format(path, &format);
    if (err != NC_NOERR) return err;

    if (format == NC_FORMAT_CLASSIC ||
        format == NC_FORMAT_CDF2 ||
        format == NC_FORMAT_CDF5) {
        driver = ncmpio_inq_driver();
    }
    if (driver == NULL) return NC_ENOTNC;

    err = driver->open(comm, path, omode, ncid, info, &ncp);
    if (err != NC_NOERR) return err;

    /* Create a NC_bb object and save its driver pointer */
    ncbbp = (NC_bb*) NCI_Malloc(sizeof(NC_bb));
    if (ncbbp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
    
    ncbbp->path = (char*) NCI_Malloc(strlen(path)+1);
    if (ncbbp->path == NULL) {
        NCI_Free(ncbbp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(ncbbp->path, path);
    ncbbp->mode       = omode;
    ncbbp->ncmpio_driver     = driver;
    ncbbp->flag       = 0;
    ncbbp->ncid = ncid;
    ncbbp->curreqid = -1;
    ncbbp->ncp        = ncp;
    MPI_Comm_dup(comm, &ncbbp->comm);

    /* Init log structure */
    err = ncmpii_log_create(ncbbp, info);
    if (err != NC_NOERR) {
        NCI_Free(ncbbp);
        return err;
    }
    ncbbp->inited = 1;

    *ncpp = ncbbp;

    return NC_NOERR;
}

int
ncbbio_close(void *ncdp)
{
    int err, status = NC_NOERR;
    NC_bb *ncbbp = (NC_bb*)ncdp;

    if (ncbbp == NULL) DEBUG_RETURN_ERROR(NC_EBADID)
    
    err = ncmpii_log_close(ncbbp);
    if (status == NC_NOERR) {
        status = err;
    }

    err = ncbbp->ncmpio_driver->close(ncbbp->ncp);
    if (status == NC_NOERR) {
        status = err;
    }

    MPI_Comm_free(&ncbbp->comm);
    NCI_Free(ncbbp->path);
    NCI_Free(ncbbp);

    return status;
}

int
ncbbio_enddef(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->enddef(ncbbp->ncp);
    if (err != NC_NOERR) return err;

    err = ncmpii_log_enddef(ncbbp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio__enddef(void       *ncdp,
              MPI_Offset  h_minfree,
              MPI_Offset  v_align,
              MPI_Offset  v_minfree,
              MPI_Offset  r_align)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->_enddef(ncbbp->ncp, h_minfree, v_align, v_minfree,
                               r_align);
    if (err != NC_NOERR) return err;

    err = ncmpii_log_enddef(ncbbp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_redef(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->redef(ncbbp->ncp);
    if (err != NC_NOERR) return err;

    err = ncmpii_log_flush(ncbbp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_begin_indep_data(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->begin_indep_data(ncbbp->ncp);
    if (err != NC_NOERR) return err;

    ncbbp->isindep = 1;

    return NC_NOERR;
}

int
ncbbio_end_indep_data(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->end_indep_data(ncbbp->ncp);
    if (err != NC_NOERR) return err;

    ncbbp->isindep = 0;

    return NC_NOERR;
}

int
ncbbio_abort(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    if (ncbbp == NULL) DEBUG_RETURN_ERROR(NC_EBADID)

    err = ncbbp->ncmpio_driver->abort(ncbbp->ncp);

    MPI_Comm_free(&ncbbp->comm);
    NCI_Free(ncbbp->path);
    NCI_Free(ncbbp);

    return err;
}

int
ncbbio_inq(void *ncdp,
          int  *ndimsp,
          int  *nvarsp,
          int  *nattsp,
          int  *xtendimp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->inq(ncbbp->ncp, ndimsp, nvarsp, nattsp, xtendimp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_inq_misc(void       *ncdp,
               int        *pathlen,
               char       *path,
               int        *num_fix_varsp,
               int        *num_rec_varsp,
               int        *striping_size,
               int        *striping_count,
               MPI_Offset *header_size,
               MPI_Offset *header_extent,
               MPI_Offset *recsize,
               MPI_Offset *put_size,
               MPI_Offset *get_size,
               MPI_Info   *info_used,
               int        *nreqs,
               MPI_Offset *usage,
               MPI_Offset *buf_size)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->inq_misc(ncbbp->ncp, pathlen, path, num_fix_varsp,
                                num_rec_varsp, striping_size, striping_count,
                                header_size, header_extent, recsize, put_size,
                                get_size, info_used, nreqs, usage, buf_size);
    if (err != NC_NOERR) return err;
    
    err = ncmpii_log_inq_put_size(ncbbp, put_size);
    if (err != NC_NOERR) return err;
    
    return NC_NOERR;
}

int
ncbbio_cancel(void *ncdp,
             int   num_req,
             int  *req_ids,
             int  *statuses)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->cancel(ncbbp->ncp, num_req, req_ids, statuses);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_wait(void *ncdp,
           int   num_reqs,
           int  *req_ids,
           int  *statuses,
           int   reqMode)
{
    int i, j, err, numreq = num_reqs;
    int *ids = req_ids, *stats = statuses;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    if (req_ids != NULL && num_reqs > 0){
        ids = NCI_Malloc(sizeof(int) * num_reqs);
        stats = NCI_Malloc(sizeof(int) * num_reqs);
        numreq = 0;
        for (i = 0; i < num_reqs; i++){
            if (req_ids[i] >= 0){
                ids[numreq++] = req_ids[i];
            }
        }
    }

    err = ncbbp->ncmpio_driver->wait(ncbbp->ncp, numreq, ids, stats, reqMode);
    if (ids != req_ids){
        if (statuses != NULL){
            for (i = j = 0; i < num_reqs; i++){
                if (req_ids[i] >= 0){
                    statuses[i] = stats[j++];
                }
                else{
                    statuses[i] = NC_NOERR;
                }
            }
        }
        NCI_Free(ids);
        NCI_Free(stats);
    }
 
    /*
    err = ncbbp->ncmpio_driver->wait(ncbbp->ncp, num_reqs, req_ids, statuses, reqMode);
    if (err != NC_NOERR) return err;
    */

    return err;
}

int
ncbbio_set_fill(void *ncdp,
               int   fill_mode,
               int  *old_fill_mode)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->set_fill(ncbbp->ncp, fill_mode, old_fill_mode);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_fill_var_rec(void      *ncdp,
                   int        varid,
                   MPI_Offset recno)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->fill_var_rec(ncbbp->ncp, varid, recno);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_def_var_fill(void       *ncdp,
                   int         varid,
                   int         no_fill,
                   const void *fill_value)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->def_var_fill(ncbbp->ncp, varid, no_fill, fill_value);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_sync_numrecs(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncbbp->ncmpio_driver->sync_numrecs(ncbbp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncbbio_sync(void *ncdp)
{
    int err;
    NC_bb *ncbbp = (NC_bb*)ncdp;
    
    err = ncmpii_log_enddef(ncbbp);
    if (err != NC_NOERR) return err;
    
    err = ncbbp->ncmpio_driver->sync(ncbbp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

