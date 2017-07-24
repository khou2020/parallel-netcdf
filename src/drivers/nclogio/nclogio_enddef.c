/*
 *  Copyright (C) 2003, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

/*
 * This file implements the corresponding APIs defined in src/dispatchers/file.c
 *
 * ncmpi_enddef()           : dispatcher->enddef()
 * ncmpi__enddef()          : dispatcher->_enddef()
 */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>  /* strtol() */
#include <string.h>  /* memset() */
#include <mpi.h>
#include <pnc_debug.h>
#include <common.h>
#include <nc.h>
#include "log.h"

int nclogio_enddef(void *ncdp) {
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;   
    
    /* Initialize the log if not initialized, otherwise, flush the log */
    if (nclogp->metalog_fd < 0) {
        err = log_create(nclogp);
        if (err != NC_NOERR){
            return err;
        }
    }
    else {
        err = log_enddef(nclogp);
        if (err != NC_NOERR){
            return err;
        }
    }

    return nclogp->ncmpio_dispatcher->enddef(nclogp->ncp);
}

int nclogio__enddef(void *ncdp, MPI_Offset h_minfree, MPI_Offset v_align, 
                MPI_Offset v_minfree, MPI_Offset r_align) {
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;   
    
    /* Initialize the log if not initialized, otherwise, flush the log */
    if (nclogp->metalog_fd < 0) {
        err = log_create(nclogp);
        if (err != NC_NOERR){
            return err;
        }
    }
    else {
        err = log_enddef(nclogp);
        if (err != NC_NOERR){
            return err;
        }
    }
    
    return nclogp->ncmpio_dispatcher->_enddef(nclogp->ncp, h_minfree, 
                v_align, v_minfree, r_align);
}

/*
 * Create log file for the log structure
 * IN    nclogp:    log structure
 */
int log_enddef(NC_Log *nclogp){   
    int i, maxdims, err;
    ssize_t ioret;
    NC *ncp = nclogp->ncp;
    NC_Log_metadataheader *headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;

    /* Highest dimension among all variables */
    maxdims = 0;    
    for(i = 0; i < ncp->vars.ndefined; i++){
        if (ncp->vars.value[i]->ndims > headerp->max_ndims){
            maxdims = ncp->vars.value[i]->ndims; 
        }
    }

    /* Update the header in max dim increased */
    if (maxdims > headerp->max_ndims){
        headerp->max_ndims = maxdims;

        /* Seek to the location of maxndims 
         * Note: location need to be updated when struct change
         */
        ioret = lseek(nclogp->metalog_fd, sizeof(NC_Log_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->basenamelen) - sizeof(headerp->num_entries) - sizeof(headerp->max_ndims), SEEK_SET);
        if (ioret < 0){
            DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
        }
        /* Overwrite num_entries
         * This marks the completion of the record
         */
        ioret = write(nclogp->metalog_fd, &headerp->max_ndims, SIZEOF_MPI_OFFSET);
        if (ioret < 0){
            err = nclogio_handle_io_error("write");
            if (err == NC_EFILE){
                err = NC_EWRITE;
            }
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != SIZEOF_MPI_OFFSET){
            err = nclogio_handle_io_error("write");
            
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
    }

    return NC_NOERR;
}

