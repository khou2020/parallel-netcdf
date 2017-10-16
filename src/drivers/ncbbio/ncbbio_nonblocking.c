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
#include <ncbbio_driver.h>

#define PUT_ARRAY_SIZE 128 /* Size of initial put list */    
#define SIZE_MULTIPLIER 2    /* When metadata buffer is full, we'll reallocate it to META_BUFFER_MULTIPLIER times the original size*/

int ncbbio_put_list_init(NC_bb *ncbbp){
    int i;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    lp->nused = 0;
    lp->nalloc = PUT_ARRAY_SIZE;
    lp->list = (NC_bb_put_req*)malloc(lp->nalloc * sizeof(NC_bb_put_req));
    lp->ids = (int*)malloc(lp->nalloc * sizeof(int));
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

int ncbbio_put_list_resize(NC_bb *ncbbp){
    int i;
    size_t nsize;
    void *ptr;
    NC_bb_put_list *lp = &(ncbbp->putlist);

    /* New size */
    nsize = lp->nalloc * SIZE_MULTIPLIER;
    
    /* Realloc list */
    ptr = realloc(lp->list, 
                            nsize * sizeof(NC_bb_put_req));
    if (ptr = NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    lp->list = (NC_bb_put_req*)ptr;
    ptr = realloc(lp->ids, nsize * sizeof(int));
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

int ncbbio_put_list_free(NC_bb *ncbbp){
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    free(lp->list);
    free(lp->ids);
    
    return 0;
}

int ncbbio_put_list_add(NC_bb *ncbbp, int *id) {
    int err;
    NC_bb_put_list *lp = &(ncbbp->putlist);

    /* Increase size if not enough */
    if (lp->nused == lp->nalloc){
        err = ncbbio_put_list_resize(ncbbp);
        if (err != NC_NOERR){
            return err;
        }
    }
     
    /* Get one form avaiable ids */
    *id = lp->ids[lp->nused++];
    lp->list[*id].valid = 1;
    
    return NC_NOERR;
}

int ncbbio_put_list_remove(NC_bb *ncbbp, int reqid){
    int i, tmp;
    int tail, idx;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    NC_bb_put_req *req;
    
    /* Free the memory */
    req = lp->list + reqid;
    req->valid = 0;
    if (req->start != NULL) {
        free(req->start);
    }
    if (req->count != NULL) {
        free(req->count);
    }
    if (req->stride != NULL) {
        free(req->stride);
    }
    if (req->imap != NULL) {
        free(req->imap);
    }
    if (req->starts != NULL) {
        for(i = 0; i < req->num; i++){
            free(req->starts[i]);
        }
        free(req->starts);
    }
    if (req->counts != NULL) {
        for(i = 0; i < req->num; i++){
            free(req->counts[i]);
        }
        free(req->counts);
    }
    
    /* Return id to the list */
    lp->ids[--lp->nused] = reqid;

    return NC_NOERR;
}

int ncbbio_put_list_add1(NC_bb *ncbbp, int *reqid, int varid,  
                        const MPI_Offset *start, const MPI_Offset *count, 
                        const MPI_Offset *stride, const MPI_Offset *imap, 
                        const void *buf, MPI_Offset bufcount,
                        MPI_Datatype buftype, int reqMode) {
    int err, id, dim;
    MPI_Offset asize;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    /* Reserve req object and get id */
    err = ncbbio_put_list_add(ncbbp, &id);
    if (err != NC_NOERR){
        return err;
    }
    
    /* Get dim of the variable, which is the size of start, count, stride ...etc */
    err = ncbbp->ncmpio_driver->inq_var(ncbbp->ncp, varid, NULL, NULL, &dim, 
        NULL, NULL, NULL, NULL, NULL);
    if (err != NC_NOERR){
        return err;
    }
    asize = dim * sizeof(MPI_Offset);
    
     
    // Copy nonblocking request info
    lp->list[id].varid = varid;
    lp->list[id].num = 0;
    lp->list[id].buf = (void*)buf;
    lp->list[id].bufcount = bufcount;
    lp->list[id].buftype = buftype;
    lp->list[id].reqMode = reqMode;
    lp->list[id].starts = NULL;
    lp->list[id].counts = NULL;
    
    // Start
    lp->list[id].start = (MPI_Offset*)malloc(asize);
    if (lp->list[id].start == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    memcpy(lp->list[id].start, start, asize);
    
    // Count
    lp->list[id].count = (MPI_Offset*)malloc(asize);
    if (lp->list[id].count == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    memcpy(lp->list[id].count, count, asize);
    
    // Stride
    if (stride != NULL){
        lp->list[id].stride = (MPI_Offset*)malloc(asize);
        if (lp->list[id].stride == NULL){
            DEBUG_RETURN_ERROR(NC_ENOMEM);
        }
        memcpy(lp->list[id].stride, stride, asize);
    }
    else{
        lp->list[id].stride = NULL;
    }

    // imap
    if (imap != NULL){
        lp->list[id].imap = (MPI_Offset*)malloc(asize);
        if (lp->list[id].imap == NULL){
            DEBUG_RETURN_ERROR(NC_ENOMEM);
        }
        memcpy(lp->list[id].imap, imap, asize);
    }
    else{
        lp->list[id].imap = NULL;
    }

    // Assign id
    if (reqid != NULL){
        *reqid = id;
    }

    return NC_NOERR;
}

int ncbbio_put_list_addn(NC_bb *ncbbp, int *reqid, int varid, int num, 
                        MPI_Offset* const  *starts, MPI_Offset* const  *counts, 
                        const void *buf, MPI_Offset bufcount,
                        MPI_Datatype buftype, int reqMode) {
    int i, err, id, dim;
    MPI_Offset asize;
    NC_bb_put_list *lp = &(ncbbp->putlist);

    /* Reserve req object and get id */
    err = ncbbio_put_list_add(ncbbp, &id);
    if (err != NC_NOERR){
        return err;
    }
    
    /* Get dim of the variable, which is the size of start, count, stride ...etc */
    err = ncbbp->ncmpio_driver->inq_var(ncbbp->ncp, varid, NULL, NULL, &dim, 
        NULL, NULL, NULL, NULL, NULL);
    if (err != NC_NOERR){
        return err;
    }
    asize = dim * sizeof(MPI_Offset);
        
    // Copy nonblocking request info
    lp->list[id].varid = varid;
    lp->list[id].num = num;
    lp->list[id].buf = (void*)buf;
    lp->list[id].bufcount = bufcount;
    lp->list[id].buftype = buftype;
    lp->list[id].reqMode = reqMode;
    lp->list[id].start = NULL;
    lp->list[id].count = NULL;
    lp->list[id].stride = NULL;
    lp->list[id].imap = NULL;
 
    // Starts
    lp->list[id].starts = (MPI_Offset**)malloc(sizeof(MPI_Offset*) * num);
    if (lp->list[id].starts == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    for(i = 0; i < num; i++){
        lp->list[id].starts[i] = (MPI_Offset*)malloc(asize);
        if (lp->list[id].starts[i] == NULL){
            DEBUG_RETURN_ERROR(NC_ENOMEM);
        }
        memcpy(lp->list[id].starts[i], starts[i], asize);
    }

    // Counts
    lp->list[id].counts = (MPI_Offset**)malloc(sizeof(MPI_Offset*) * num);
    if (lp->list[id].counts == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    for(i = 0; i < num; i++){
        lp->list[id].counts[i] = (MPI_Offset*)malloc(asize);
        if (lp->list[id].counts[i] == NULL){
            DEBUG_RETURN_ERROR(NC_ENOMEM);
        }
        memcpy(lp->list[id].counts[i], counts[i], asize);
    }
 
    // Assign id
    if (reqid != NULL){
        *reqid = id;
    }

    return NC_NOERR;
}


int ncbbio_handle_put_req(NC_bb *ncbbp, int reqid){
    int err, status = NC_NOERR;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    NC_bb_put_req *req;

    // Filter invalid reqid
    if (reqid > lp->nalloc) {
        DEBUG_RETURN_ERROR(NC_EINVAL_REQUEST);
    }

    // Locate the req object
    req = lp->list + reqid;
    
    // Filter invalid reqid
    if (!req->valid){
        DEBUG_RETURN_ERROR(NC_EINVAL_REQUEST);
    }

    // Create log entry of pending request
    if (req->num > 0){
        err = ncbbio_put_varn(ncbbp, req->varid, req->num, req->starts, req->counts, 
                req->buf, req->bufcount, req->buftype,
                 req->reqMode);
    }
    else{
       err = ncbbio_put_var(ncbbp, req->varid, req->start, req->count, 
            req->stride, req->imap, req->buf, req->bufcount, req->buftype,
             req->reqMode);
    }
    if (status == NC_NOERR){
        status = err;
    }

    // Return req object to the pool
    err = ncbbio_put_list_remove(ncbbp, reqid);
    if (status == NC_NOERR){
        status = err;
    }

    return status;
}

int ncbbio_handle_all_put_req(NC_bb *ncbbp){
    int i, err, status = NC_NOERR;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    // Search through req object list for valid objects */
    for(i = 0; i < lp->nalloc; i++){
        if (lp->list[i].valid){
            err = ncbbio_handle_put_req(ncbbp, i);
            if (status == NC_NOERR){
                status = err;
            }
        }
    }

    return NC_NOERR;
}

int ncbbio_remove_all_put_req(NC_bb *ncbbp){
    int i, err, status = NC_NOERR;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    // Search through req object list for valid objects */
    for(i = 0; i < lp->nalloc; i++){
        if (lp->list[i].valid){
            err = ncbbio_put_list_remove(ncbbp, i);
            if (status == NC_NOERR){
                status = err;
            }
        }
    }

    return NC_NOERR;
}


