#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <sys/types.h>
#include "ncx.h"
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <pnc_debug.h>
#include <common.h>
#include <pnetcdf.h>
#include <ncbbio_driver.h>

void ncbbio_extract_hint(NC_bb *ncbbp, MPI_Info info){
    int flag;
    char value[MPI_MAX_INFO_VAL];

    /* Extract hints */
    ncbbp->hints = NC_LOG_HINT_DEL_ON_CLOSE | NC_LOG_HINT_FLUSH_ON_READ | 
                    NC_LOG_HINT_FLUSH_ON_SYNC;
    MPI_Info_get(info, "nc_bb_dirname", MPI_MAX_INFO_VAL - 1,
                 value, &flag);
    if (flag) {
        strncpy(ncbbp->logbase, value, PATH_MAX);
    }
    else {
        strncpy(ncbbp->logbase, ".", PATH_MAX);    
    }
    MPI_Info_get(info, "nc_bb_overwrite", MPI_MAX_INFO_VAL - 1, 
                 value, &flag);
    if (flag && strcasecmp(value, "enable") == 0){
        ncbbp->hints |= NC_LOG_HINT_LOG_OVERWRITE;
    }
    MPI_Info_get(info, "nc_bb_check", MPI_MAX_INFO_VAL - 1, 
                 value, &flag);
    if (flag && strcasecmp(value, "enable") == 0){
        ncbbp->hints |= NC_LOG_HINT_LOG_CHECK;
    }
    MPI_Info_get(info, "nc_bb_del_on_close", MPI_MAX_INFO_VAL - 1, 
                 value, &flag);
    if (flag && strcasecmp(value, "disable") == 0){
        ncbbp->hints ^= NC_LOG_HINT_DEL_ON_CLOSE;
    }
    MPI_Info_get(info, "nc_bb_flush_buffer_size", MPI_MAX_INFO_VAL - 1,
                 value, &flag);
    if (flag){
        long int bsize = strtol(value, NULL, 0);
        if (bsize < 0) {
            bsize = 0;
        }
        ncbbp->flushbuffersize = (size_t)bsize; // Unit: byte 
    }
    else{
        ncbbp->flushbuffersize = 0; // <= 0 means unlimited}
    }
}
