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
#define SIZE_MULTIPLIER 2    /* When metadata buffer is full, we'll NCI_Reallocate it to META_BUFFER_MULTIPLIER times the original size*/

int ncbbio_put_list_init(NC_bb *ncbbp){
    int i;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    lp->nused = 0;
    lp->nalloc = PUT_ARRAY_SIZE;
    lp->list = (NC_bb_put_req*)NCI_Malloc(lp->nalloc * sizeof(NC_bb_put_req));
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

int ncbbio_put_list_resize(NC_bb *ncbbp){
    int i;
    size_t nsize;
    void *ptr;
    NC_bb_put_list *lp = &(ncbbp->putlist);

    /* New size */
    nsize = lp->nalloc * SIZE_MULTIPLIER;
    
    /* Realloc list */
    ptr = NCI_Realloc(lp->list, 
                            nsize * sizeof(NC_bb_put_req));
    if (ptr = NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    lp->list = (NC_bb_put_req*)ptr;
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

int ncbbio_put_list_free(NC_bb *ncbbp){
    NC_bb_put_list *lp = &(ncbbp->putlist);
    
    NCI_Free(lp->list);
    NCI_Free(lp->ids);
    
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
    lp->list[*id].ready = 0;
    lp->list[*id].status = NC_NOERR;
    
    return NC_NOERR;
}

int ncbbio_put_list_remove(NC_bb *ncbbp, int reqid){
    int i, tmp;
    int tail, idx;
    NC_bb_put_list *lp = &(ncbbp->putlist);
    NC_bb_put_req *req;
    
    /* Return id to the list */
    lp->ids[--lp->nused] = reqid;

    return NC_NOERR;
}

int ncbbio_handle_put_req(NC_bb *ncbbp, int reqid, int *stat){
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

    if (!req->ready){
        ncbbio_log_flush(ncbbp);
    }
    
    if (!req->ready){
        printf("Error\n");
    }

    if (stat != NULL){
        *stat = req->status;
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
            err = ncbbio_handle_put_req(ncbbp, i, NULL);
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


