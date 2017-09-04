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
#include <nclogio_driver.h>

int
nclogio_create(MPI_Comm     comm,
             const char  *path,
             int          cmode,
             int          ncid,
             MPI_Info     info,
             void       **ncpp)  /* OUT */
{
    int err;
    void *ncp=NULL;
    NC_Log *nclogp;
    PNC_driver *driver=NULL;

    /* TODO: use comde to determine the true driver */
    driver = ncmpio_inq_driver();
    if (driver == NULL) return NC_ENOTNC;

    err = driver->create(comm, path, cmode, ncid, info, &ncp);
    if (err != NC_NOERR) return err;

    /* Create a NC_Log object and save its driver pointer */
    nclogp = (NC_Log*) NCI_Malloc(sizeof(NC_Log));
    if (nclogp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
   
    nclogp->path = (char*) NCI_Malloc(strlen(path)+1);
    if (nclogp->path == NULL) {
        NCI_Free(nclogp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(nclogp->path, path);
    nclogp->mode       = cmode;
    nclogp->ncmpio_driver     = driver;
    nclogp->flag       = 0;
    nclogp->ncp        = ncp;
    MPI_Comm_dup(comm, &nclogp->comm);
    
    /* Init log structure */
    err = log_create(nclogp);
    if (err != NC_NOER) {
        NCI_Free(nclogp);
        return err;
    }

    *ncpp = nclogp;

    return NC_NOERR;
}

int
nclogio_open(MPI_Comm     comm,
           const char  *path,
           int          omode,
           int          ncid,
           MPI_Info     info,
           void       **ncpp)
{
    int err, format, flag;
    void *ncp=NULL;
    NC_Log *nclogp;
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

    /* Create a NC_Log object and save its driver pointer */
    nclogp = (NC_Log*) NCI_Malloc(sizeof(NC_Log));
    if (nclogp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
    
    nclogp->path = (char*) NCI_Malloc(strlen(path)+1);
    if (nclogp->path == NULL) {
        NCI_Free(nclogp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(nclogp->path, path);
    nclogp->mode       = omode;
    nclogp->ncmpio_driver     = driver;
    nclogp->flag       = 0;
    nclogp->ncp        = ncp;
    MPI_Comm_dup(comm, &nclogp->comm);

    /* Init log structure */
    err = log_open(nclogp, info);
    if (err != NC_NOER) {
        NCI_Free(nclogp);
        return err;
    }

    *ncpp = nclogp;

    return NC_NOERR;
}

int
nclogio_close(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;

    if (nclogp == NULL) DEBUG_RETURN_ERROR(NC_EBADID)

    err = nclogp->ncmpio_driver->close(nclogp->ncp);

    MPI_Comm_free(&nclogp->comm);
    NCI_Free(nclogp->path);
    NCI_Free(nclogp);

    return err;
}

int
nclogio_enddef(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->enddef(nclogp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio__enddef(void       *ncdp,
              MPI_Offset  h_minfree,
              MPI_Offset  v_align,
              MPI_Offset  v_minfree,
              MPI_Offset  r_align)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->_enddef(nclogp->ncp, h_minfree, v_align, v_minfree,
                               r_align);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_redef(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->redef(nclogp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_begin_indep_data(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->begin_indep_data(nclogp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_end_indep_data(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->end_indep_data(nclogp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_abort(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    if (nclogp == NULL) DEBUG_RETURN_ERROR(NC_EBADID)

    err = nclogp->ncmpio_driver->abort(nclogp->ncp);

    MPI_Comm_free(&nclogp->comm);
    NCI_Free(nclogp->path);
    NCI_Free(nclogp);

    return err;
}

int
nclogio_inq(void *ncdp,
          int  *ndimsp,
          int  *nvarsp,
          int  *nattsp,
          int  *xtendimp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->inq(nclogp->ncp, ndimsp, nvarsp, nattsp, xtendimp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_inq_misc(void       *ncdp,
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
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->inq_misc(nclogp->ncp, pathlen, path, num_fix_varsp,
                                num_rec_varsp, striping_size, striping_count,
                                header_size, header_extent, recsize, put_size,
                                get_size, info_used, nreqs, usage, buf_size);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_cancel(void *ncdp,
             int   num_req,
             int  *req_ids,
             int  *statuses)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->cancel(nclogp->ncp, num_req, req_ids, statuses);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_wait(void *ncdp,
           int   num_reqs,
           int  *req_ids,
           int  *statuses,
           int   reqMode)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->wait(nclogp->ncp, num_reqs, req_ids, statuses, reqMode);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_set_fill(void *ncdp,
               int   fill_mode,
               int  *old_fill_mode)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->set_fill(nclogp->ncp, fill_mode, old_fill_mode);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_fill_var_rec(void      *ncdp,
                   int        varid,
                   MPI_Offset recno)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->fill_var_rec(nclogp->ncp, varid, recno);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_def_var_fill(void       *ncdp,
                   int         varid,
                   int         no_fill,
                   const void *fill_value)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->def_var_fill(nclogp->ncp, varid, no_fill, fill_value);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_sync_numrecs(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->sync_numrecs(nclogp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_sync(void *ncdp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->sync(nclogp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

