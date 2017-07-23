/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <pnc_debug.h>
#include <common.h>
#include <nc.h>
#include "log.h"

int nclogio_sync(void *ncdp) {
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;   
    
    if(nclogp->metalog_fd >= 0){
        err = nclogio_flush(nclogp); 
        if (err != NC_NOERR){
            return err;
        }
    }
    
    return nclogp->ncmpio_dispatcher->sync(nclogp->ncp);
}


