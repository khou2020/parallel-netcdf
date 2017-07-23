/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

/*
 * This file implements the corresponding APIs defined in src/dispatchers/file.c
 *
 * ncmpi_close()            : dispatcher->close()
 */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <mpi.h>
#include <pnc_debug.h>
#include <common.h>
#include <nc.h>
#include "log.h"

int nclogio_close(void *ncdp) {
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;   
    NC_Log_metadataheader* headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
    NC *ncp = (NC*)nclogp->ncp;

    /* If log file is created, flush the log */
    if (nclogp->metalog_fd >= 0){
        /* Commit to CDF file */
        if (headerp->num_entries > 0){
            nclogp->isflushing = 1;
            log_flush(ncp);
            nclogp->isflushing = 0;
        }

        /* Close log file */
        err = close(nclogp->metalog_fd);
        err |= close(nclogp->datalog_fd);
        if (err < 0){
            DEBUG_RETURN_ERROR(nclogio_handle_io_error("close"));        
        }

        /* Delete log files if delete flag is set */
        if (nclogp->hints & NC_LOG_HINT_DEL_ON_CLOSE){
            unlink(nclogp->datalogpath);
            unlink(nclogp->metalogpath);
        }
    }

    /* Free meta data buffer and metadata offset list*/
    nclogio_log_buffer_free(&nclogp->metadata);
    nclogio_log_sizearray_free(&nclogp->entrydatasize);
 
    err = nclogp->ncmpio_dispatcher->close(nclogp->ncp);
    if (err != NC_NOERR){
        return err;
    }

    /* Delete log structure */
    NCI_Free(nclogp);

    return NC_NOERR;
}


