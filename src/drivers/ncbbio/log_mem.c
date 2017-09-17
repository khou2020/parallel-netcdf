#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <common.h>
#include <pnc_debug.h>
#include <common.h>
#include <pnetcdf.h>
#include <ncbbio_driver.h>

#define LOG_BUFFER_SIZE 1024 /* Size of initial metadata buffer */
#define LOG_ARRAY_SIZE 32 /* Size of initial metadata offset list */    
#define SIZE_MULTIPLIER 20    /* When metadata buffer is full, we'll NCI_Reallocate it to META_BUFFER_MULTIPLIER times the original size*/

/* 
 * Initialize a variable sized buffer
 * IN   bp: buffer structure to be initialized
 */
int ncmpii_log_buffer_init(NC_bb_buffer * bp){
    bp->buffer = NCI_Malloc(LOG_BUFFER_SIZE);
    if (bp->buffer == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    bp->nalloc = LOG_BUFFER_SIZE;
    bp->nused = 0;
    return NC_NOERR;
}

/* 
 * Free the variable sized buffer
 * IN   bp: buffer structure to be freed
 */
void ncmpii_log_buffer_free(NC_bb_buffer * bp){
    NCI_Free(bp->buffer);
}

/*
 * Allocate space in the variable sized buffer
 * This function works as NCI_Malloc in the metadata buffer
 * IN    bp:    buffer structure
 * IN    size:    size required in the buffer
 */
char* ncmpii_log_buffer_alloc(NC_bb_buffer *bp, size_t size) {
    char* ret;

    /* Expand buffer if needed 
     * bp->nused is the size currently in use
     * bp->nalloc is the size of internal buffer
     * If the remaining size is less than the required size, we reallocate the buffer
     */
    if (bp->nalloc < bp->nused + size) {
        /* 
         * We don't know how large the required size is, loop until we have enough space
         * Must make sure realloc successed before increasing bp->nalloc
         */
        size_t newsize = bp->nalloc;
        while (newsize < bp->nused + size) {
            /* (new size) = (old size) * (META_BUFFER_MULTIPLIER) */
            newsize *= SIZE_MULTIPLIER;
        }
        /* ret is used to temporaryly hold the allocated buffer so we don't lose ncbbp->metadata.buffer if allocation fails */
        ret = (char*)NCI_Realloc(bp->buffer, newsize);
        /* If not enough memory */
        if (ret == NULL) {
            return ret;
        }
        /* Point to the new buffer and update nalloc */
        bp->buffer = ret;
        bp->nalloc = newsize;
    }
    
    /* Increase used buffer size and return the allocated space */
    ret = bp->buffer + bp->nused;
    bp->nused += size;

    return ret;
}

/* 
 * Initialize log entry array
 * IN   ep: array to be initialized
 */
int ncmpii_log_sizearray_init(NC_bb_sizearray *sp){
    sp->values = (size_t*)NCI_Malloc(LOG_ARRAY_SIZE * sizeof(NC_bb_metadataentry*));
    if (sp->values == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    sp->nalloc = LOG_ARRAY_SIZE;
    sp->nused = 0;
    return NC_NOERR;
}

/* 
 * Free the log entry array
 * IN   ep: array to be freed
 */
void ncmpii_log_sizearray_free(NC_bb_sizearray *sp){
    NCI_Free(sp->values);
}

/*
 * Append entry to array
 * IN    ep:    array structure
 * IN    ent:    entry to be added
 */
int ncmpii_log_sizearray_append(NC_bb_sizearray *sp, size_t size) {
    size_t *ret;

    /* Expand array if needed 
     * sp->nused is the size currently in use
     * sp->nalloc is the size of internal buffer
     * If the remaining size is less than the required size, we reallocate the buffer
     */
    if (sp->nalloc < sp->nused + 1) {
        /* 
         * Must make sure realloc successed before increasing sp->nalloc
         * (new size) = (old size) * (META_BUFFER_MULTIPLIER) 
         */
        size_t newsize = sp->nalloc * SIZE_MULTIPLIER;
        /* ret is used to temporaryly hold the allocated buffer so we don't lose ncbbp->metadata.buffer if allocation fails */
        ret = (size_t*)NCI_Realloc(sp->values, newsize * sizeof(size_t));
        /* If not enough memory */
        if (ret == NULL) {
            return NC_ENOMEM;
        }
        /* Point to the new buffer and update nalloc */
        sp->values = ret;
        sp->nalloc = newsize;
    }
    
    /* Add entry to tail */
    sp->values[sp->nused++] = size;

    return NC_NOERR;
}

