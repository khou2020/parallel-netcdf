/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <common.h>
#include <pnc_debug.h>
#include <common.h>
#include <pnetcdf.h>
#include <ncdwio_driver.h>

#define PUT_ARRAY_SIZE 128 /* Size of initial put list */    
#define SIZE_MULTIPLIER 2    /* When metadata buffer is full, we'll NCI_Reallocate it to META_BUFFER_MULTIPLIER times the original size*/

int ncdwio_put_list_init(NC_dw *ncdwp){
    int i;
    NC_dw_put_list *lp = &(ncdwp->putlist);
    
    lp->nused = 0;
    lp->nalloc = PUT_ARRAY_SIZE;
    lp->list = (NC_dw_put_req*)NCI_Malloc(lp->nalloc * sizeof(NC_dw_put_req));
    lp->ids = (int*)NCI_Malloc(lp->nalloc * sizeof(int));
    if (lp->list == NULL || lp->ids == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    
    /* Assign increasing unique id */
    for(i = 0; i < lp->nalloc; i++){
        lp->ids[i] = i;
        lp->list[i].valid = 0;
    }

    return NC_NOERR;
}

int ncdwio_put_list_resize(NC_dw *ncdwp){
    int i;
    size_t nsize;
    void *ptr;
    NC_dw_put_list *lp = &(ncdwp->putlist);

    /* New size */
    nsize = lp->nalloc * SIZE_MULTIPLIER;
    
    /* Realloc list */
    ptr = NCI_Realloc(lp->list, 
                            nsize * sizeof(NC_dw_put_req));
    if (ptr = NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    lp->list = (NC_dw_put_req*)ptr;
    ptr = NCI_Realloc(lp->ids, nsize * sizeof(int));
    if (ptr = NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    lp->ids = (int*)ptr;
 
    /* Assign increasing unique id */
    for(i = lp->nalloc; i < nsize; i++){
        lp->ids[i] = i;
        lp->list[i].valid = 0;
    }
    
    lp->nalloc = nsize;
    
    return 0;
}

int ncdwio_put_list_free(NC_dw *ncdwp){
    NC_dw_put_list *lp = &(ncdwp->putlist);
    
    NCI_Free(lp->list);
    NCI_Free(lp->ids);
    
    return 0;
}

int ncdwio_put_list_add(NC_dw *ncdwp, int *id) {
    int err;
    NC_dw_put_list *lp = &(ncdwp->putlist);

    /* Increase size if not enough */
    if (lp->nused == lp->nalloc){
        err = ncdwio_put_list_resize(ncdwp);
        if (err != NC_NOERR){
            return err;
        }
    }
     
    /* Get one form avaiable ids */
    *id = lp->ids[lp->nused++];
    lp->list[*id].valid = 1;
    lp->list[*id].ready = 0;
    lp->list[*id].status = NC_NOERR;
    
    return NC_NOERR;
}

int ncdwio_put_list_remove(NC_dw *ncdwp, int reqid){
    int i, tmp;
    int tail, idx;
    NC_dw_put_list *lp = &(ncdwp->putlist);
    
    /* Mark entry as invalid */
    lp->list[reqid].valid = 0;

    /* Return id to the list */
    lp->ids[--lp->nused] = reqid;

    return NC_NOERR;
}

int ncdwio_handle_put_req(NC_dw *ncdwp, int reqid, int *stat){
    int err, status = NC_NOERR;
    NC_dw_put_list *lp = &(ncdwp->putlist);
    NC_dw_put_req *req;

    // Filter invalid reqid
    if (reqid > lp->nalloc) {
        if (stat != NULL) {
            *stat = NC_EINVAL_REQUEST;
        }
        return status;
    }

    // Locate the req object
    req = lp->list + reqid;
    
    // Filter invalid reqid
    if (!req->valid){
        if (stat != NULL) {
            *stat = NC_EINVAL_REQUEST;
        }
        return status;
    }

    if (!req->ready){
        // check return val
        ncdwio_log_flush(ncdwp);
    }
    
    if (!req->ready){
        printf("Fatal error: nonblocking request not in log file\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    if (stat != NULL){
        *stat = req->status;
    }

    // Recycle req object to the pool
    err = ncdwio_put_list_remove(ncdwp, reqid);
    if (status == NC_NOERR){
        status = err;
    }

    return status;
}

int ncdwio_handle_all_put_req(NC_dw *ncdwp){
    int i, err, status = NC_NOERR;
    NC_dw_put_list *lp = &(ncdwp->putlist);
    
    // Search through req object list for valid objects */
    for(i = 0; i < lp->nalloc; i++){
        if (lp->list[i].valid){
            err = ncdwio_handle_put_req(ncdwp, i, NULL);
            if (status == NC_NOERR){
                status = err;
            }
        }
    }

    return NC_NOERR;
}

int ncdwio_cancel_put_req(NC_dw *ncdwp, int reqid, int *stat){
    int i, err, status = NC_NOERR;
    NC_dw_put_list *lp = &(ncdwp->putlist);
    NC_dw_put_req *req;

    // Filter invalid reqid
    if (reqid > lp->nalloc) {
        if (stat != NULL) {
            *stat = NC_EINVAL_REQUEST;
        }
        return status;
    }

    // Locate the req object
    req = lp->list + reqid;
    
    // Filter invalid reqid
    if (!req->valid){
        if (stat != NULL) {
            *stat = NC_EINVAL_REQUEST;
        }
        return status;
    }

    if (req->ready){
        if (stat != NULL) {
            *stat = NC_EFLUSHED;
        }
    }
    else{
        if (stat != NULL){
            *stat = NC_NOERR;
        }
        
        // Cancel log entries
        for(i = req->entrystart; i < req->entryend; i++) {
            ncdwp->metaidx.entries[i].valid = 0;
        }
    }
    
    // Return req object to the pool
    err = ncdwio_put_list_remove(ncdwp, reqid);
    if (status == NC_NOERR){
        status = err;
    }

    return status;
}

int ncdwio_cancel_all_put_req(NC_dw *ncdwp){
    int i, err, status = NC_NOERR;
    NC_dw_put_list *lp = &(ncdwp->putlist);
    
    // Search through req object list for valid objects */
    for(i = 0; i < lp->nalloc; i++){
        if (lp->list[i].valid){
            err = ncdwio_cancel_put_req(ncdwp, i, NULL);
            if (status == NC_NOERR){
                status = err;
            }
        }
    }

    return NC_NOERR;
}


