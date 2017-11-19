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
#include <ncdwio_driver.h>

int
ncdwio_create(MPI_Comm     comm,
             const char  *path,
             int          cmode,
             int          ncid,
             MPI_Info     info,
             void       **ncpp)  /* OUT */
{
    int err;
    void *ncp=NULL;
    NC_dw *ncdwp;
    PNC_driver *driver=NULL;

    /* TODO: use comde to determine the true driver */
    driver = ncmpio_inq_driver();
    if (driver == NULL) return NC_ENOTNC;

    err = driver->create(comm, path, cmode, ncid, info, &ncp);
    if (err != NC_NOERR) return err;

    /* Create a NC_dw object and save its driver pointer */
    ncdwp = (NC_dw*) NCI_Malloc(sizeof(NC_dw));
    if (ncdwp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
   
    ncdwp->path = (char*) NCI_Malloc(strlen(path)+1);
    if (ncdwp->path == NULL) {
        NCI_Free(ncdwp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(ncdwp->path, path);
    ncdwp->mode = cmode;
    ncdwp->ncmpio_driver = driver;
    ncdwp->flag = 0;
    ncdwp->ncid = ncid;
    ncdwp->curreqid = -1;
    ncdwp->niget = 0;
    ncdwp->isindep = 0;
    ncdwp->ncp = ncp;
    ncdwp->recdimsize = 0;
    ncdwp->recdimid = -1;
    ncdwp->max_ndims = 0;
    MPI_Comm_dup(comm, &(ncdwp->comm));
    MPI_Info_dup(info, &(ncdwp->info));
    ncdwio_extract_hint(ncdwp, info);

    /* Log init delaied to enddef */
    ncdwp->inited = 0;
    
    *ncpp = ncdwp;

    return NC_NOERR;
}

int
ncdwio_open(MPI_Comm     comm,
           const char  *path,
           int          omode,
           int          ncid,
           MPI_Info     info,
           void       **ncpp)
{
    int err, format, flag;
    void *ncp=NULL;
    NC_dw *ncdwp;
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

    /* Create a NC_dw object and save its driver pointer */
    ncdwp = (NC_dw*) NCI_Malloc(sizeof(NC_dw));
    if (ncdwp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
    
    ncdwp->path = (char*) NCI_Malloc(strlen(path)+1);
    if (ncdwp->path == NULL) {
        NCI_Free(ncdwp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(ncdwp->path, path);
    ncdwp->mode = omode;
    ncdwp->ncmpio_driver = driver;
    ncdwp->flag = 0;
    ncdwp->ncid = ncid;
    ncdwp->curreqid = -1;
    ncdwp->niget = 0;
    ncdwp->isindep = 0;
    ncdwp->ncp = ncp;
    ncdwp->recdimsize = 0;
    ncdwp->recdimid = -1;
    ncdwp->max_ndims = 0;
    MPI_Comm_dup(comm, &(ncdwp->comm));
    MPI_Info_dup(info, &(ncdwp->info));
    ncdwio_extract_hint(ncdwp, info);

    /* Init log structure if not read only */
    if (omode != NC_NOWRITE ){
        err = ncdwio_log_create(ncdwp, info);
        if (err != NC_NOERR) {
            NCI_Free(ncdwp);
            return err;
        }
        ncdwio_put_list_init(ncdwp);
        ncdwio_metaidx_init(ncdwp);
        ncdwp->inited = 1;
    }
    else{
        ncdwp->inited = 0;
    }
    *ncpp = ncdwp;

    return NC_NOERR;
}

int
ncdwio_close(void *ncdp)
{
    int err, status = NC_NOERR;
    NC_dw *ncdwp = (NC_dw*)ncdp;

    if (ncdwp == NULL) DEBUG_RETURN_ERROR(NC_EBADID)
    
    /* 
     * Close log files
     * Log is only created after enddef
     */
    if (ncdwp->inited){
        err = ncdwio_log_close(ncdwp);
        if (status == NC_NOERR) {
            status = err;
        }
        ncdwio_put_list_free(ncdwp);
        ncdwio_metaidx_free(ncdwp);
    }

    err = ncdwp->ncmpio_driver->close(ncdwp->ncp);
    if (status == NC_NOERR) {
        status = err;
    }

    MPI_Comm_free(&(ncdwp->comm));
    MPI_Info_free(&(ncdwp->info));
    NCI_Free(ncdwp->path);
    NCI_Free(ncdwp);

    return status;
}

int
ncdwio_enddef(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->enddef(ncdwp->ncp);
    if (err != NC_NOERR) return err;

    /* Init or update log structure */
    if (ncdwp->inited){
        /* Update log with new information */
        err = ncdwio_log_enddef(ncdwp);
        if (err != NC_NOERR) return err;
    }
    else {
        /* Init log structure */
        err = ncdwio_log_create(ncdwp, ncdwp->info);
        if (err != NC_NOERR) {
            //NCI_Free(ncdwp);
            return err;
        }
        ncdwio_put_list_init(ncdwp);
        ncdwio_metaidx_init(ncdwp);
        ncdwp->inited = 1;
    }

    return NC_NOERR;
}

int
ncdwio__enddef(void       *ncdp,
              MPI_Offset  h_minfree,
              MPI_Offset  v_align,
              MPI_Offset  v_minfree,
              MPI_Offset  r_align)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->_enddef(ncdwp->ncp, h_minfree, v_align, v_minfree,
                               r_align);
    if (err != NC_NOERR) return err;

    /* Init or update log structure */
    if (ncdwp->inited){
        /* Update log with new information */
        err = ncdwio_log_enddef(ncdwp);
        if (err != NC_NOERR) return err;
    }
    else {
        /* Init log structure */
        err = ncdwio_log_create(ncdwp, ncdwp->info);
        if (err != NC_NOERR) {
            //NCI_Free(ncdwp);
            return err;
        }
        ncdwio_put_list_init(ncdwp);
        ncdwio_metaidx_init(ncdwp);
        ncdwp->inited = 1;
    }

    return NC_NOERR;
}

int
ncdwio_redef(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    /* 
     * Flush log entries to the file system on redefine 
     */
    if (ncdwp->inited) {
        err = ncdwio_log_flush(ncdwp);
        if (err != NC_NOERR) return err;
    }

    err = ncdwp->ncmpio_driver->redef(ncdwp->ncp);
    if (err != NC_NOERR) return err;
    
    return NC_NOERR;
}

int
ncdwio_begin_indep_data(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->begin_indep_data(ncdwp->ncp);
    if (err != NC_NOERR) return err;

    /* Independent mode */
    ncdwp->isindep = 1;

    return NC_NOERR;
}

int
ncdwio_end_indep_data(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->end_indep_data(ncdwp->ncp);
    if (err != NC_NOERR) return err;

    /* Collective mode */
    ncdwp->isindep = 0;

    return NC_NOERR;
}

int
ncdwio_abort(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    if (ncdwp == NULL) DEBUG_RETURN_ERROR(NC_EBADID)

    err = ncdwp->ncmpio_driver->abort(ncdwp->ncp);

    MPI_Comm_free(&(ncdwp->comm));
    NCI_Free(ncdwp->path);
    NCI_Free(ncdwp);

    return err;
}

int
ncdwio_inq(void *ncdp,
          int  *ndimsp,
          int  *nvarsp,
          int  *nattsp,
          int  *xtendimp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->inq(ncdwp->ncp, ndimsp, nvarsp, nattsp, xtendimp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncdwio_inq_misc(void       *ncdp,
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
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->inq_misc(ncdwp->ncp, pathlen, path, num_fix_varsp,
                                num_rec_varsp, striping_size, striping_count,
                                header_size, header_extent, recsize, put_size,
                                get_size, info_used, nreqs, usage, buf_size);
    if (err != NC_NOERR) return err;
    
    if (ncdwp->inited) {
        /* Add the size of data log to reflect pending put in the log */
        if (put_size != NULL){
            *put_size += (MPI_Offset)ncdwp->datalogsize - 8;
            
        }
        
        /* Add number of write requests to nreqs */
        if (nreqs != NULL){
            *nreqs += ncdwp->putlist.nused;
            
        }
    }

    /* Export bb related settings */
    if (info_used != NULL){
        ncdwio_export_hint(ncdwp, *info_used);
    }

    return NC_NOERR;
}

int
ncdwio_cancel(void *ncdp,
             int   num_req,
             int  *req_ids,
             int  *statuses)
{
    int i, j, err, status = NC_NOERR;
    int nput, tmp, stat;
    int *swaps;
    NC_dw *ncdwp = (NC_dw*)ncdp;
   
   /*
    * Nonblocking put is reserved for log flush
    * We pick negative ids for put
    * Forward only positive ids (get oepration) to ncmpio
    * Process put operation (negative id) only
    */
 
    if (num_req == NC_REQ_ALL || num_req == NC_PUT_REQ_ALL || num_req == NC_GET_REQ_ALL){
        if (num_req == NC_REQ_ALL || num_req == NC_PUT_REQ_ALL){
            err = ncdwio_cancel_all_put_req(ncdwp);
            if (status == NC_NOERR){
                status = err;
            }
        }
        if (num_req == NC_REQ_ALL || num_req == NC_GET_REQ_ALL){
            err = ncdwp->ncmpio_driver->cancel(ncdwp->ncp, num_req, NULL, NULL);
            if (status == NC_NOERR){
                status = err;
            }
        }

        return status;
    }
        
    swaps = (int*)NCI_Malloc(sizeof(int) * num_req);

    nput = 0;
    for(i = 0; i < num_req; i++){
        if (req_ids[i] < 0){
            swaps[nput] = i;
            tmp = req_ids[i];
            req_ids[i] = req_ids[nput];
            req_ids[nput++] = tmp;
        }
    }
    
    if (nput > 0){
        for(i = 0; i < nput; i++){
            err = ncdwio_cancel_put_req(ncdwp, -(req_ids[i] + 1), &stat);
            if (status == NC_NOERR){
                status = err;
            }
            if (statuses != NULL){
                statuses[i] = stat;
            }
        }
    }

    if (num_req > nput){
        err = ncdwp->ncmpio_driver->cancel(ncdwp->ncp, num_req - nput, req_ids + nput, statuses + nput);
        if (status == NC_NOERR){
            status = err;
        }
    }
    
    for(i = nput - 1; i > -1; i--){
        j = swaps[i];
        tmp = req_ids[i];
        req_ids[i] = req_ids[j];
        req_ids[j] = tmp;
    }

    if (statuses != NULL){
        for(i = nput - 1; i > -1; i--){
            j = swaps[i];
            tmp = statuses[i];
            statuses[i] = statuses[j];
            statuses[j] = tmp;
        }
    }
            
    NCI_Free(swaps);
 
    return status;
}

int
ncdwio_wait(void *ncdp,
           int   num_reqs,
           int  *req_ids,
           int  *statuses,
           int   reqMode)
{
    int i, j, err, status = NC_NOERR;
    int nput, tmp, stat;
    int *swaps;
    NC_dw *ncdwp = (NC_dw*)ncdp;
   
   /*
    * Nonblocking put is reserved for log flush
    * We pick negative ids for put
    * Forward only positive ids (get oepration) to ncmpio
    * Process put operation (negative id) only
    */
 
    if (num_reqs == NC_REQ_ALL || num_reqs == NC_PUT_REQ_ALL || num_reqs == NC_GET_REQ_ALL){
        if (num_reqs == NC_REQ_ALL || num_reqs == NC_PUT_REQ_ALL){
            err = ncdwio_handle_all_put_req(ncdwp);
            if (status == NC_NOERR){
                status = err;
            }
        }
        if (num_reqs == NC_REQ_ALL || num_reqs == NC_GET_REQ_ALL){
            err = ncdwp->ncmpio_driver->wait(ncdwp->ncp, num_reqs, NULL, NULL, reqMode);
            if (status == NC_NOERR){
                status = err;
            }
        }

        return status;
    }
    
    // Replace the name swaps to more meaningful, eg. w_ids, r_ids
    // Add more comments to describe
    swaps = (int*)NCI_Malloc(sizeof(int) * num_reqs);

    nput = 0;
    for(i = 0; i < num_reqs; i++){
        if (req_ids[i] < 0){
            swaps[nput] = i;
            tmp = req_ids[i];
            req_ids[i] = req_ids[nput];
            req_ids[nput++] = tmp;
        }
    }
    
    if (nput > 0){
        for(i = 0; i < nput; i++){
            err = ncdwio_handle_put_req(ncdwp, -(req_ids[i] + 1), &stat);
            if (status == NC_NOERR){
                status = err;
            }
            if (statuses != NULL){
                statuses[i] = stat;
            }
        }
    }

    if (num_reqs > nput){
        /* 
         * Flush the log if there is any get operation
         * If there are ids of get in req_ids
         */
        if (ncdwp->inited){
            err = ncdwio_log_flush(ncdwp);
            if (status == NC_NOERR){
                status = err;
            }
        }
        err = ncdwp->ncmpio_driver->wait(ncdwp->ncp, num_reqs - nput, req_ids + nput, statuses + nput, reqMode);
        if (status == NC_NOERR){
            status = err;
        }
    }
    
    for(i = nput - 1; i > -1; i--){
        j = swaps[i];
        tmp = req_ids[i];
        req_ids[i] = req_ids[j];
        req_ids[j] = tmp;
    }

    if (statuses != NULL){
        for(i = nput - 1; i > -1; i--){
            j = swaps[i];
            tmp = statuses[i];
            statuses[i] = statuses[j];
            statuses[j] = tmp;
        }
    }
            
    NCI_Free(swaps);
 
    return status;
}

int
ncdwio_set_fill(void *ncdp,
               int   fill_mode,
               int  *old_fill_mode)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->set_fill(ncdwp->ncp, fill_mode, old_fill_mode);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncdwio_fill_var_rec(void      *ncdp,
                   int        varid,
                   MPI_Offset recno)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->fill_var_rec(ncdwp->ncp, varid, recno);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncdwio_def_var_fill(void       *ncdp,
                   int         varid,
                   int         no_fill,
                   const void *fill_value)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->def_var_fill(ncdwp->ncp, varid, no_fill, fill_value);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncdwio_sync_numrecs(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    err = ncdwp->ncmpio_driver->sync_numrecs(ncdwp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
ncdwio_sync(void *ncdp)
{
    int err;
    NC_dw *ncdwp = (NC_dw*)ncdp;
    
    /* Flush on sync */
    if (ncdwp->inited) {
        err = ncdwio_log_flush(ncdwp);
        if (err != NC_NOERR) return err;
    }

    err = ncdwp->ncmpio_driver->sync(ncdwp->ncp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

