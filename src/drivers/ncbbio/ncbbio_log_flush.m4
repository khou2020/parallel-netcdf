dnl Process this m4 file to produce 'C' language file.
dnl
dnl If you see this line, you can ignore the next one.
/* Do not edit this file. It is produced from the corresponding .m4 source */
dnl
/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* "$Id$" */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
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
#include <stdio.h>
#include <stdlib.h>
#include <ncbbio_driver.h>

define(`PREPAREPARAM',dnl
`dnl
                /* start, count, stride */
                start = (MPI_Offset*)(entryp + 1);
                count = start + entryp->ndims;
                stride = count + entryp->ndims;

                /* Convert from log type to MPI type used by pnetcdf library
                 * Log spec has different enum of types than MPI
                 */
                if (entryp->itype == NC_LOG_TYPE_TEXT) {
                    buftype = MPI_CHAR;
                }
                else if (entryp->itype == NC_LOG_TYPE_SCHAR) {
                    buftype = MPI_SIGNED_CHAR;
                }
                else if (entryp->itype == NC_LOG_TYPE_SHORT) {
                    buftype = MPI_SHORT;
                }
                else if (entryp->itype == NC_LOG_TYPE_INT) {
                    buftype = MPI_INT;
                }
                else if (entryp->itype == NC_LOG_TYPE_FLOAT) {
                    buftype = MPI_FLOAT;
                }
                else if (entryp->itype == NC_LOG_TYPE_DOUBLE) {
                    buftype = MPI_DOUBLE;
                }
                else if (entryp->itype == NC_LOG_TYPE_UCHAR) {
                    buftype = MPI_UNSIGNED_CHAR;
                }
                else if (entryp->itype == NC_LOG_TYPE_USHORT) {
                    buftype = MPI_UNSIGNED_SHORT;
                }
                else if (entryp->itype == NC_LOG_TYPE_UINT) {
                    buftype = MPI_UNSIGNED;
                }
                else if (entryp->itype == NC_LOG_TYPE_LONGLONG) {
                    buftype = MPI_LONG_LONG_INT;
                }
                else if (entryp->itype == NC_LOG_TYPE_ULONGLONG) {
                    buftype = MPI_UNSIGNED_LONG_LONG;
                }
                else {
                    DEBUG_RETURN_ERROR(NC_EINVAL);
                }
                
                /* Determine API_Kind */
                if (entryp->api_kind == NC_LOG_API_KIND_VARA){
                    stride = NULL;    
                }

                /* Translate varid to varid */
                /*
                err = ncmpii_NC_lookupvar(ncp, entryp->varid, &varid);
                if (status == NC_NOERR) {
                    status = err;
                }
                */

')dnl


define(`FLUSHBATCH',dnl
`dnl
            t2 = MPI_Wtime();
            /* 
             * Read data to buffer
             * We read only what needed by pending requests
             */
            ioret = read(ncbbp->datalog_fd, databuffer, databufferused); 
            if (ioret < 0) {
                ioret = ncmpii_error_posix2nc("read");
                if (ioret == NC_EFILE){
                    ioret = NC_EREAD;
                }
                DEBUG_RETURN_ERROR(ioret);
            }
            if (ioret != databufferused){
                DEBUG_RETURN_ERROR(NC_EBADLOG);
            }
            t3 = MPI_Wtime();
            ncbbp->flush_read_time += t3 - t2;

            for(i = lb; i < ub; i++){
                
                ip = ncbbp->metaidx.entries + i;
                
                if (ip->valid) {
                    PREPAREPARAM   
                    
                    t2 = MPI_Wtime();
                    
                    /* Replay event with non-blocking call */
                    err = ncbbp->ncmpio_driver->iput_var(ncbbp->ncp, entryp->varid, start, count, stride, NULL, (void*)(databuffer + entryp->data_off - databufferidx), -1, buftype, reqids + i, NC_REQ_WR | NC_REQ_NBI | NC_REQ_HL);
                    if (status == NC_NOERR) {
                        status = err;
                    }
                                    
                    t3 = MPI_Wtime();
                    ncbbp->flush_replay_time += t3 - t2;
                }
     
                /* Move to next position */
                entryp = (NC_bb_metadataentry*)(((char*)entryp) + entryp->esize);
            }

            t2 = MPI_Wtime();
            
            /* 
             * Wait must be called first or previous data will be corrupted
             */
            if (ncbbp->isindep) {
                err = ncbbp->ncmpio_driver->wait(ncbbp->ncp, ub - lb, reqids + lb, stats + lb, NC_REQ_INDEP); 
            }
            else{
                err = ncbbp->ncmpio_driver->wait(ncbbp->ncp, ub - lb, reqids + lb, stats + lb, NC_REQ_COLL);
            }
            if (status == NC_NOERR) {
                status = err;
            }
            
            for(i = lb; i < ub; i++){
                ip = ncbbp->metaidx.entries + i;
                
                if (ip->valid) {
                    if (ip->reqid >= 0){
                        ncbbp->putlist.list[ip->reqid].status = stats[i];
                        ncbbp->putlist.list[ip->reqid].ready = 1;
                    }
                }
            }
 
            t3 = MPI_Wtime();
            ncbbp->flush_replay_time += t3 - t2;

            /* Update batch status */
            databufferidx += databufferused;
            databufferused = 0;
')dnl

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    ncbbp:    log structure
 */
int split_iput(NC_bb *ncbbp, int varid, int ndims, MPI_Offset *start, MPI_Offset *count, MPI_Offset *stride, MPI_Datatype buftype, MPI_Offset dataoff, MPI_Offset datalen, size_t buffersize, void *buffer) {
    int i, err, reqmode;
    MPI_Offset count1, count2;
    MPI_Offset start1, start2;
    MPI_Offset datalen1, datalen2;

    /* Flush when buffer is enough to fit */
    if (buffersize >= datalen){
        double t1, t2, t3;
        ssize_t ioret;
        
        t1 = MPI_Wtime();

        /* Read buffer into memory */
        ioret = read(ncbbp->datalog_fd, buffer, datalen); 
        if (ioret < 0) {
            ioret = ncmpii_error_posix2nc("read");
            if (ioret == NC_EFILE){
                ioret = NC_EREAD;
            }
            DEBUG_RETURN_ERROR(ioret);
        }
        if (ioret != datalen){
            DEBUG_RETURN_ERROR(NC_EBADLOG);
        }
        
        t2 = MPI_Wtime();
        ncbbp->flush_read_time += t2 - t1;

        /* 
         * Replay event with non-blocking call 
         * Blocking call will be intercepted by logging
         * Must use non-blocking call even if only one instance at a time
         */
        //err = ncmpii_igetput_varm(ncp, varid, start, count, stride, NULL, buffer, -1, buftype, NULL, WRITE_REQ, 0, 0);
        reqmode = NC_REQ_WR | NC_REQ_BLK | NC_REQ_HL;
        if (ncbbp->isindep){
            reqmode |= NC_REQ_COLL;
        }
        else{
            reqmode |= NC_REQ_INDEP;
        }
        err = ncbbp->ncmpio_driver->put_var(ncbbp->ncp, varid, start, count, stride, NULL, buffer, -1, buftype, reqmode);
        
        if (err != NC_NOERR) {
            return err;
        }
        
        /* Wait for request */ 
        /*
        if (NC_indep(ncp)) {
            err = ncmpii_wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, INDEP_IO);
            if (err != NC_NOERR) {
                return err;
            }
        }
        else{
            err = ncmpii_wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
            if (err != NC_NOERR) {
                return err;
            }
        }
        */

        t3 = MPI_Wtime();
        ncbbp->flush_read_time += t3 - t2;
    }
    else{
        for (i = 0; i < ndims; i++) {
            if (count[i] > 1){
                break;
            }
        }
        
        /*
         * We don't split data type
         * If buffer is not enough for even one entry, return error
         */
        if ( i == ndims) {
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
        err = split_iput(ncbbp, varid, ndims, start, count, stride, buftype, dataoff, datalen1, buffersize, buffer);
        if (err != NC_NOERR) {
            return err;
        }
        start[i] = start2;
        count[i] = count2;
        err = split_iput(ncbbp, varid, ndims, start, count, stride, buftype, dataoff + datalen1, datalen2, buffersize, buffer);
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
 * IN    ncbbp:    log structure
 */
int log_flush(NC_bb *ncbbp) {
    int i, j, lb, ub, err, fd, status = NC_NOERR;
    int *reqids, *stats;
    double t1, t2, t3;
    size_t databufferused, databuffersize, databufferidx;
    ssize_t ioret;
    NC_bb_metadataentry *entryp;
    MPI_Offset *start, *count, *stride;
    MPI_Datatype buftype;
    char *databuffer;
    NC_bb_metadataheader *headerp;
    NC_bb_metadataptr *ip;

    t1 = MPI_Wtime();
    
    
    /* Read datalog in to memory */
    /* 
     * Prepare data buffer
     * We determine the data buffer size according to:
     * hints, size of data log, the largest size of single record
     * 0 in hint means no limit
     * (Buffer size) = max((largest size of single record), min((size of data log), (size specified in hint)))
     */
    databuffersize = ncbbp->datalogsize;
    if (ncbbp->logflushbuffersize > 0 && databuffersize > ncbbp->logflushbuffersize){
        databuffersize = ncbbp->logflushbuffersize;
    }
    /* Allocate buffer */
    databuffer = (char*)NCI_Malloc(databuffersize);
    if(databuffer == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }

    /* Seek to the start position of first data record */
    ioret = lseek(ncbbp->datalog_fd, 8, SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_error_posix2nc("lseek"));
    }
    /* Initialize buffer status */
    databufferidx = 8;
    databufferused = 0;

    reqids = (int*)NCI_Malloc(ncbbp->entrydatasize.nused * sizeof(int));
    stats = (int*)NCI_Malloc(ncbbp->entrydatasize.nused * sizeof(int));

    /* 
     * Iterate through meta log entries
     * i is entries scaned for size
     * j is entries replayed
     */
    headerp = (NC_bb_metadataheader*)ncbbp->metadata.buffer;
    entryp = (NC_bb_metadataentry*)(ncbbp->metadata.buffer + headerp->entry_begin);
    for (lb = ub = 0; ub < ncbbp->entrydatasize.nused; ub++){
            
        /* Process current batch if data buffer can not accomodate next one */
        if(databufferused + ncbbp->entrydatasize.values[ub] >= databuffersize){
FLUSHBATCH
            lb = ub;
        }
               
        /* 
         * If buffer not enough for single entry, we must split it
         * An oversized entry must have triggered a flush on current batch, so entire buffer is free to use
         * We must have entryp points to current entry
         */
        if (ncbbp->entrydatasize.values[ub] > databuffersize){
            ip = ncbbp->metaidx.entries + ub;
            
            if (ip->valid){
                PREPAREPARAM
                           
                /* Replay event in parts */
                err = split_iput(ncbbp, entryp->varid, entryp->ndims, start, count, stride, buftype, entryp->data_off, entryp->data_len, databuffersize, databuffer);
                if (status == NC_NOERR) {
                    status = err;
                }
                
                if (ip->reqid >= 0){
                    ncbbp->putlist.list[ip->reqid].status = err;    
                }
                ncbbp->putlist.list[ip->reqid].ready = 1;    

                           
                /* Update batch status */
                databufferidx += entryp->data_len;
                databufferused = 0;

                /* Skip this entry on batch flush */
                entryp = (NC_bb_metadataentry*)(((char*)entryp) + entryp->esize);
                lb++;
            }
        }
        else{
            /* 
             * Record current entry size
             * entryp is updated when replay is done, we don't have entryp here
             */
            databufferused += ncbbp->entrydatasize.values[ub];
        }

    }
    
    /* Process last batch if there are unflushed entries */
    if(databufferused > 0){
FLUSHBATCH
    }

    /* Free the data buffer */ 
    NCI_Free(databuffer);
    NCI_Free(reqids);
    NCI_Free(stats);

    t2 = MPI_Wtime();
    ncbbp->flush_total_time += t2 - t1;
    
    return status;
}


