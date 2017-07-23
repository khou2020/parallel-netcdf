dnl Process this m4 file to produce 'C' language file.
dnl
dnl If you see this line, you can ignore the next one.
/* Do not edit this file. It is produced from the corresponding .m4 source */
dnl
/*
 *  Copyright (C) 2003, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* "$Id$" */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include "nc.h"
#include "ncx.h"
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <pnetcdf.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pnc_debug.h>
#include <common.h>
#include <pnetcdf.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>

define(`PREPAREPARAM',dnl
`dnl
                /* start, count, stride */
                start = (MPI_Offset*)(entryp + 1);
                count = start + entryp->ndims;
                stride = count + entryp->ndims;

                /* Convert from log type to MPI type used by pnetcdf library
                 * Log spec has different enum of types than MPI
                 */
                switch (entryp->itype) {
                case NC_LOG_TYPE_TEXT:
                    buftype = MPI_CHAR;
                    break;
                case NC_LOG_TYPE_SCHAR:
                    buftype = MPI_SIGNED_CHAR;
                    break;
                case NC_LOG_TYPE_SHORT:
                    buftype = MPI_SHORT;
                    break;
                case NC_LOG_TYPE_INT:
                    buftype = MPI_INT;
                    break;
                case NC_LOG_TYPE_FLOAT:
                    buftype = MPI_FLOAT;
                    break;
                case NC_LOG_TYPE_DOUBLE:
                    buftype = MPI_DOUBLE;
                    break;
                case NC_LOG_TYPE_UCHAR:
                    buftype = MPI_UNSIGNED_CHAR;
                    break;
                case NC_LOG_TYPE_USHORT:
                    buftype = MPI_UNSIGNED_SHORT;
                    break;
                case NC_LOG_TYPE_UINT:
                    buftype = MPI_UNSIGNED;
                    break;
                case NC_LOG_TYPE_LONGLONG:
                    buftype = MPI_LONG_LONG_INT;
                    break;
                case NC_LOG_TYPE_ULONGLONG:
                    buftype = MPI_UNSIGNED_LONG_LONG;
                    break;
                default:
                    DEBUG_RETURN_ERROR(NC_EINVAL);
                }
                
                /* Determine API_Kind */
                if (entryp->api_kind == NC_LOG_API_KIND_VARA){
                    api = API_VARA;
                    stride = NULL;    
                }
                else{
                    api = API_VARS;
                }

                /* Translate varid to varp */
                err = ncmpio_NC_lookupvar(ncp, entryp->varid, &varp);
                if (err != NC_NOERR){
                    return err;
                }

')dnl


define(`FLUSHBATCH',dnl
`dnl
#ifdef PNETCDF_DEBUG
            tbegin = MPI_Wtime();
#endif
            /* 
             * Read data to buffer
             * We read only what needed by pending requests
             */
            ioret = read(nclogp->datalog_fd, databuffer, databufferused); 
            if (ioret < 0) {
                ioret = nclogio_handle_io_error("read");
                if (ioret == NC_EFILE){
                    ioret = NC_EREAD;
                }
                DEBUG_RETURN_ERROR(ioret);
            }
            if (ioret != databufferused){
                DEBUG_RETURN_ERROR(NC_EBADLOG);
            }

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            tread += tend - tbegin;
            tbegin = MPI_Wtime();
#endif
            for(; j < i; j++){
                PREPAREPARAM   
                
                /* Replay event with non-blocking call */
                err = nclogp->ncmpio_dispatcher->iput_var(ncp, varp->varid, start, count, stride, NULL, (void*)(databuffer + entryp->data_off - databufferidx), -1, buftype, NULL, api, buftype);
                if (err != NC_NOERR) {
                    return err;
                }
                
                /* Move to next position */
                entryp = (NC_Log_metadataentry*)(((char*)entryp) + entryp->esize);
            }

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            treplay += tend - tbegin;
            tbegin = MPI_Wtime();
#endif
            /* 
             * Wait must be called first or previous data will be corrupted
             */
            if (NC_indep(ncp)) {
                err = nclogp->ncmpio_dispatcher->wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, INDEP_IO);
                if (err != NC_NOERR) {
                    return err;
                }
            }
            else{
                err = nclogp->ncmpio_dispatcher->wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
                if (err != NC_NOERR) {
                    return err;
                }
            }

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            twait += tend - tbegin;
#endif
            /* Update batch status */
            databufferidx += databufferused;
            databufferused = 0;
')dnl

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    nclogp:    log structure
 */
                
int split_iput(NC_Log *nclogp, NC_var *varp, MPI_Offset *start, MPI_Offset *count, MPI_Offset *stride, MPI_Datatype buftype, MPI_Offset dataoff, MPI_Offset datalen, size_t buffersize, void *buffer, api_kind api) {
    int i, err;
    MPI_Offset count1, count2;
    MPI_Offset start1, start2;
    MPI_Offset datalen1, datalen2;
    NC *ncp = (NC*)nclogp->ncp;

    /* Flush when buffer is enough to fit */
    if (buffersize >= datalen){
        ssize_t ioret;
        
        /* Read buffer into memory */
        ioret = read(nclogp->datalog_fd, buffer, datalen); 
        if (ioret < 0) {
            ioret = nclogio_handle_io_error("read");
            if (ioret == NC_EFILE){
                ioret = NC_EREAD;
            }
            DEBUG_RETURN_ERROR(ioret);
        }
        if (ioret != datalen){
            DEBUG_RETURN_ERROR(NC_EBADLOG);
        }
 
        /* 
         * Replay event with non-blocking call 
         * Blocking call will be intercepted by logging
         * Must use non-blocking call even if only one instance at a time
         */
        err = nclogp->ncmpio_dispatcher->iput_var(ncp, varp->varid, start, count, stride, NULL, buffer, -1, buftype, NULL, api, buftype);
        if (err != NC_NOERR) {
            return err;
        }
        
        /* Wait for request */ 
        if (NC_indep(ncp)) {
            err = nclogp->ncmpio_dispatcher->wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, INDEP_IO);
            if (err != NC_NOERR) {
                return err;
            }
        }
        else{
            err = nclogp->ncmpio_dispatcher->wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
            if (err != NC_NOERR) {
                return err;
            }
        }
    }
    else{
        for (i = 0; i < varp->ndims; i++) {
            if (count[i] > 1){
                break;
            }
        }
        
        /*
         * We don't split data type
         * If buffer is not enough for even one entry, return error
         */
        if ( i == varp->ndims) {
            DEBUG_RETURN_ERROR(NC_ENOMEM);   
        }

        /* Calculate split */
        count1 = count[i] / 2;
        count2 = count[i] - count1;
        start1 = start[i];
        start2 = start[i] + count1;
        datalen1 =  datalen / (count1 + count2) * count1;
        datalen2 = datalen - datalen1;

        /* 
         * First half then second half
         * To read the data log continuously, we must do first half first
         */
        start[i] = start1;
        count[i] = count1;
        err = split_iput(nclogp, varp, start, count, stride, buftype, dataoff, datalen1, buffersize, buffer, api);
        if (err != NC_NOERR) {
            return err;
        }
        start[i] = start2;
        count[i] = count2;
        err = split_iput(nclogp, varp, start, count, stride, buftype, dataoff + datalen1, datalen2, buffersize, buffer, api);
        if (err != NC_NOERR) {
            return err;
        }
        /* Restore start and count value */
        start[i] = start1;
        count[i] = count1 + count2;
        
        return NC_NOERR;
    }
}

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    nclogp:    log structure
 */
int log_flush(NC_Log *nclogp) {
    int i, j, err, fd;
    api_kind api;
    size_t databufferused, databuffersize, databufferidx;
    ssize_t ioret;
    NC_Log_metadataentry *entryp;
    MPI_Offset *start, *count, *stride;
    MPI_Datatype buftype;
    char *databuffer;
    NC_Log_metadataheader* headerp;
    NC *ncp = (NC*)nclogp->ncp;
    NC_var *varp;

#ifdef PNETCDF_DEBUG
    int rank;
    double tstart, tend, tread = 0, twait = 0, treplay = 0, tbegin, ttotal;
    
    MPI_Comm_rank(ncp->comm, &rank);

    tstart = MPI_Wtime();
#endif

    /* Turn on the flushing flag so non-blocking call won't be logged */
    nclogp->isflushing = 1;

    /* Read datalog in to memory */
   
    /* 
     * Prepare data buffer
     * We determine the data buffer size according to:
     * hints, size of data log, the largest size of single record
     * 0 in hint means no limit
     * (Buffer size) = max((largest size of single record), min((size of data log), (size specified in hint)))
     */
    databuffersize = nclogp->datalogsize;
    if (nclogp->flushbuffersize > 0 && databuffersize > nclogp->flushbuffersize){
        databuffersize = nclogp->flushbuffersize;
    }
    /* Allocate buffer */
    databuffer = (char*)NCI_Malloc(databuffersize);
    if(databuffer == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }

    /* Seek to the start position of first data record */
    ioret = lseek(nclogp->datalog_fd, 8, SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
    }
    /* Initialize buffer status */
    databufferidx = 8;
    databufferused = 0;

    /* 
     * Iterate through meta log entries
     * i is entries scaned for size
     * j is entries replayed
     */
    headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
    entryp = (NC_Log_metadataentry*)(nclogp->metadata.buffer + headerp->entry_begin);
    for (i = j = 0; i < nclogp->entrydatasize.nused; i++){
            
        /* Process current batch if data buffer can not accomodate next one */
        if(databufferused + nclogp->entrydatasize.values[i] >= databuffersize){
FLUSHBATCH
        }
               
        /* 
         * If buffer not enough for single entry, we must split it
         * An oversized entry must have triggered a flush on current batch, so entire buffer is free to use
         * We must have entryp points to current entry
         */
        if (nclogp->entrydatasize.values[i] > databuffersize){
            PREPAREPARAM
                       
            /* Replay event in parts */
            err = split_iput(nclogp, varp, start, count, stride, buftype, entryp->data_off, entryp->data_len, databuffersize, databuffer, api);
            if (err != NC_NOERR) {
                return err;
            }
            
            /* Update batch status */
            databufferidx += entryp->data_len;
            databufferused = 0;

            /* Skip this entry on batch flush */
            entryp = (NC_Log_metadataentry*)(((char*)entryp) + entryp->esize);
            j++;
        }
        else{
            /* 
             * Record current entry size
             * entryp is updated when replay is done, we don't have entryp here
             */
            databufferused += nclogp->entrydatasize.values[i];
        }

    }
    
    /* Process last batch if there are unflushed entries */
    if(databufferused > 0){
FLUSHBATCH
    }

    /* Free the data buffer */ 
    NCI_Free(databuffer);
    
    /* Flusg complete. Turn off the flushing flag, enable logging on non-blocking call */
    nclogp->isflushing = 0;

#ifdef PNETCDF_DEBUG
    tend = MPI_Wtime();
    ttotal = tend - tstart;

    if (rank == 0){
        printf("Size of data log:       %lld\n", nclogp->datalogsize);
        printf("Size of data buffer:    %lld\n", databuffersize);
        printf("Size of metadata log:   %lld\n", nclogp->metadata.nused);
        printf("Number of log entries:  %lld\n", headerp->num_entries);
    
        printf("Time reading data (s):              %lf\n", tread);
        printf("Time replaying log (s):             %lf\n", treplay);
        printf("Time waiting non-blocking put (s):  %lf\n", twait);
        printf("Total time flushing (s):            %lf\n", ttotal);
    }
#endif
    
    return NC_NOERR;
}

/*
 * Commit the log into cdf file and delete the log
 * User can call this to force a commit without closing
 * It work by flush and re-initialize the log structure
 * IN    nclogp:    log structure
 */
int nclogio_flush(NC_Log *nclogp) {
    int err;
    size_t ioret;
    NC *ncp = (NC*)nclogp->ncp;
    NC_Log_metadataheader *headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;

    /* Nothing to replay if nothing have been written */
    if (headerp->num_entries == 0){
        return NC_NOERR;
    }
    
    /* Replay log file */
    nclogp->isflushing = 1;
    if ((err = log_flush(nclogp)) != NC_NOERR) {
        nclogp->isflushing = 0;
        return err;
    }
    nclogp->isflushing = 0;
    
    /* Reset log status */
    
    /* Set num_entries to 0 */
    headerp->num_entries = 0;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(nclogp->metalog_fd, sizeof(NC_Log_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->basenamelen) - sizeof(headerp->num_entries), SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of flush
     */
    ioret = write(nclogp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ioret < 0){
        err = nclogio_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != SIZEOF_MPI_OFFSET){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Reset metadata buffer and entry array status */
    nclogp->metadata.nused = headerp->entry_begin;
    nclogp->entrydatasize.nused = 0;

    /* Rewind data log file descriptors and reset the size */
    ioret = lseek(nclogp->datalog_fd, 8, SEEK_SET);
    if (ioret != 8){
        DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
    }
    nclogp->datalogsize = 8;

    return NC_NOERR;
}

