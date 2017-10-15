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

#define PUT_ARRAY_SIZE 32 /* Size of initial put list */    
#define SIZE_MULTIPLIER 2    /* When metadata buffer is full, we'll NCI_Reallocate it to META_BUFFER_MULTIPLIER times the original size*/

int ncbbio_put_list_init(NC_bb *ncbbp){
    NC_bb_put_list *lp = &ncbbp->putlist;

    lp->nused = 0;
    lp->nalloc = PUT_ARRAY_SIZE;
    lp->list = (NC_bb_put_req*)NCI_Malloc(lp->nalloc * sizeof(NC_bb_put_req));
    lp->ids = (int*)NCI_Malloc(lp->nalloc * sizeof(int));
}

int ncbbio_put_list_resize(NC_bb *ncbbp){
    int i;
    size_t nsize;
    NC_bb_put_list *lp = &ncbbp->putlist;

    nsize = lp->nalloc * SIZE_MULTIPLIER;
    lp->list = (NC_bb_put_req*)NCI_Realloc(lp->list, 
                            nsize * sizeof(NC_bb_put_req));
    lp->ids = (int*)NCI_Realloc(lp->ids, nsize * sizeof(int));
    for(i = lp->nalloc; i < nsize; i++){
        lp->ids[i] = i;
    }
    lp->nalloc = nsize;
}

int ncbbio_put_list_free(NC_bb *ncbbp){
    NC_bb_put_list *lp = &ncbbp->putlist;

    lp->nused = 0;
    lp->nalloc = 0;
    NCI_Free(lp->list);
    NCI_Free(lp->ids);
}

int ncbbio_put_list_add(NC_bb *ncbbp, int *reqid, int varid, 
                        const MPI_Offset *start, const MPI_Offset *count, 
                        const MPI_Offset *stride, const MPI_Offset *imap, 
                        const void *buf, MPI_Offset bufcount,
                        MPI_Datatype buftype, int reqMode) {
    int err, id, dim;
    MPI_Offset asize;
    NC_bb_put_list *lp = &ncbbp->putlist;

    if (lp->nused == lp->nalloc){
        ncbbio_put_list_resize(ncbbp);
    }

    err = ncbbp->ncmpio_driver->inq_var(ncbbp->ncp, varid, NULL, NULL, &dim, 
        NULL, NULL, NULL, NULL, NULL);
    if (err != NC_NOERR){
        return err;
    }
    asize = dim * sizeof(MPI_Offset);
    
    id = lp->ids[lp->nused];

    // Copy nonblocking request info
    lp->list[id].idx = lp->nused;
    lp->list[id].id = id;
    lp->list[id].valid = 1;
    lp->list[id].buf = buf;
    lp->list[id].bufcount = bufcount;
    lp->list[id].buftype = buftype;
    lp->list[id].reqMode = reqMode;
    if (start != NULL){
        lp->list[id].start = (MPI_Offset*)NCI_Malloc(asize);
        memcpy(lp->list[id].start, start, asize);
    }
    else{
        lp->list[id].start = NULL;
    }
    if (count != NULL){
        lp->list[id].count = (MPI_Offset*)NCI_Malloc(asize);
        memcpy(lp->list[id].count, count, asize);
    }
    else{
        lp->list[id].count = NULL;
    }
    if (stride != NULL){
        lp->list[id].stride = (MPI_Offset*)NCI_Malloc(asize);
        memcpy(lp->list[id].stride, stride, asize);
    }
    else{
        lp->list[id].stride = NULL;
    }
    if (imap != NULL){
        lp->list[id].imap = (MPI_Offset*)NCI_Malloc(asize);
        memcpy(lp->list[id].imap, imap, asize);
    }
    else{
        lp->list[id].imap = NULL;
    }

    lp->nused++;

    if (reqid != NULL){
        *reqid = id;
    }

    return NC_NOERR;
}

int ncbbio_put_list_remove(NC_bb *ncbbp, int reqid){
    int tmp;
    int tail, idx;
    NC_bb_put_list *lp = &ncbbp->putlist;
    NC_bb_put_req *req;

    tail = lp->ids[lp->nused - 1];
    idx = lp->list[reqid].idx;
    lp->list[tail].idx = idx;

    tmp = lp->ids[idx];
    lp->ids[idx] = lp->ids[lp->nused - 1];
    lp->ids[lp->nused - 1] = tmp;
    lp->nused--;

    req = lp->list + reqid;
    req->valid = 0;
    if (req->start != NULL) {
        NCI_Free(req->start);
    }
    if (req->count != NULL) {
        NCI_Free(req->count);
    }
    if (req->stride != NULL) {
        NCI_Free(req->stride);
    }
    if (req->imap != NULL) {
        NCI_Free(req->imap);
    }

    return NC_NOERR;
}

int ncbbio_handle_put_req(NC_bb *ncbbp, int reqid){
    int err, status = NC_NOERR;
    NC_bb_put_list *lp = &ncbbp->putlist;
    NC_bb_put_req *req;

    req = lp->list + reqid;
    err = ncbbio_put_var(ncbbp, req->varid, req->start, req->count, 
        req->stride, req->imap, req->buf, req->bufcount, req->buftype,
         req->reqMode);
    if (status == NC_NOERR){
        status = err;
    }

    err = ncbbio_put_list_remove(ncbbp, reqid);
    if (status == NC_NOERR){
        status = err;
    }

    return status;
}

int ncbbio_handle_all_put_req(NC_bb *ncbbp){
    int i;
    int *reqids;
    NC_bb_put_list *lp = &ncbbp->putlist;

    reqids = (int*)NCI_Malloc(sizeof(int) * lp->nused);
    memcpy(lp->ids, reqids, sizeof(int) * lp->nused);
    for(i = 0; i < lp->nused; i++){
        ncbbio_handle_put_req(ncbbp, reqids[i]);
    }
    NCI_Free(reqids);

    return NC_NOERR;
}

