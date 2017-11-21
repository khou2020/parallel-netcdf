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
#include <stdio.h>
#include <ncdwio_driver.h>

/* 
 * NOTE: This function is deprecated, oversized request now goes directly to the file
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    ncdwp:    log structure
 */
int split_iput(NC_dw *ncdwp, int varid, int ndims, MPI_Offset *start, MPI_Offset *count, MPI_Offset *stride, MPI_Datatype buftype, MPI_Offset dataoff, MPI_Offset datalen, size_t buffersize, void *buffer) {
    int i, err, reqmode;
    MPI_Offset count1, count2;
    MPI_Offset start1, start2;
    MPI_Offset datalen1, datalen2;

    /* Flush when buffer is enough to fit */
    if (buffersize >= datalen){
        ssize_t ioret;
#ifdef PNETCDF_PROFILING
        double t1, t2, t3;

        t1 = MPI_Wtime();
#endif
        /* Read buffer into memory */
        err = ncdwio_bufferedfile_read(ncdwp->datalog_fd, buffer, datalen); 
        if (err != NC_NOERR){
            return err;
        }
        /*
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
        */
#ifdef PNETCDF_PROFILING
        t2 = MPI_Wtime();
        ncdwp->flush_data_rd_time += t2 - t1;
#endif

        /* 
         * Replay event with non-blocking call 
         * Blocking call will be intercepted by logging
         * Must use non-blocking call even if only one instance at a time
         */
        reqmode = NC_REQ_WR | NC_REQ_BLK | NC_REQ_HL;
        if (ncdwp->isindep){
            reqmode |= NC_REQ_INDEP;
        }
        else{
            reqmode |= NC_REQ_COLL;
        }
        err = ncdwp->ncmpio_driver->put_var(ncdwp->ncp, varid, start, count, stride, NULL, buffer, -1, buftype, reqmode);
        
        if (err != NC_NOERR) {
            return err;
        }
#ifdef PNETCDF_PROFILING
        t3 = MPI_Wtime();
        ncdwp->flush_put_time += t3 - t2;
#endif
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
        err = split_iput(ncdwp, varid, ndims, start, count, stride, buftype, dataoff, datalen1, buffersize, buffer);
        if (err != NC_NOERR) {
            return err;
        }
        start[i] = start2;
        count[i] = count2;
        err = split_iput(ncdwp, varid, ndims, start, count, stride, buftype, dataoff + datalen1, datalen2, buffersize, buffer);
        if (err != NC_NOERR) {
            return err;
        }
        /* Restore start and count value */
        start[i] = start1;
        count[i] = count1 + count2;
        
        return NC_NOERR;
    }
}

/* Convert from log type to MPI type used by pnetcdf library
 * Log spec has different enum of types than MPI
 */
int logtype2mpitype(int type, MPI_Datatype *buftype){
    /* Convert from log type to MPI type used by pnetcdf library
     * Log spec has different enum of types than MPI
     */
    if (type == NC_LOG_TYPE_TEXT) {
        *buftype = MPI_CHAR;
    }
    else if (type == NC_LOG_TYPE_SCHAR) {
        *buftype = MPI_SIGNED_CHAR;
    }
    else if (type == NC_LOG_TYPE_SHORT) {
        *buftype = MPI_SHORT;
    }
    else if (type == NC_LOG_TYPE_INT) {
        *buftype = MPI_INT;
    }
    else if (type == NC_LOG_TYPE_FLOAT) {
        *buftype = MPI_FLOAT;
    }
    else if (type == NC_LOG_TYPE_DOUBLE) {
        *buftype = MPI_DOUBLE;
    }
    else if (type == NC_LOG_TYPE_UCHAR) {
        *buftype = MPI_UNSIGNED_CHAR;
    }
    else if (type == NC_LOG_TYPE_USHORT) {
        *buftype = MPI_UNSIGNED_SHORT;
    }
    else if (type == NC_LOG_TYPE_UINT) {
        *buftype = MPI_UNSIGNED;
    }
    else if (type == NC_LOG_TYPE_LONGLONG) {
        *buftype = MPI_LONG_LONG_INT;
    }
    else if (type == NC_LOG_TYPE_ULONGLONG) {
        *buftype = MPI_UNSIGNED_LONG_LONG;
    }
    else {
        DEBUG_RETURN_ERROR(NC_EINVAL);
    }

    return NC_NOERR;
}

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    ncdwp:    log structure
 */
int log_flush(NC_dw *ncdwp) {
    int i, j, lb, ub, err, fd, status = NC_NOERR;
    int *reqids, *stats;
    size_t databufferused, databuffersize, dataread;
    ssize_t ioret;
    NC_dw_metadataentry *entryp;
    MPI_Offset *start, *count, *stride;
    MPI_Datatype buftype;
    char *databuffer, *databufferoff;
    NC_dw_metadataheader *headerp;
    NC_dw_metadataptr *ip;
#ifdef PNETCDF_PROFILING
    double t1, t2, t3, t4;
    
    t1 = MPI_Wtime();
#endif

    /* Read datalog in to memory */
    /* 
     * Prepare data buffer
     * We determine the data buffer size according to:
     * hints, size of data log, the largest size of single record
     * 0 in hint means no limit
     * (Buffer size) = max((largest size of single record), min((size of data log), (size specified in hint)))
     */
    databuffersize = ncdwp->datalogsize;
    if (ncdwp->flushbuffersize > 0 && databuffersize > ncdwp->flushbuffersize){
        databuffersize = ncdwp->flushbuffersize;
    }
    if (ncdwp->max_buffer < databuffersize){
        ncdwp->max_buffer = databuffersize;
    }

    /* Allocate buffer */
    databuffer = (char*)NCI_Malloc(databuffersize);
    if(databuffer == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }

    /* Seek to the start position of first data record */
    err = ncdwio_bufferedfile_seek(ncdwp->datalog_fd, 8, SEEK_SET);
    if (err != NC_NOERR){
        return err;
    }

    /* Initialize buffer status */
    databufferused = 0;
    dataread = 0;

    reqids = (int*)NCI_Malloc(ncdwp->entrydatasize.nused * SIZEOF_INT);
    stats = (int*)NCI_Malloc(ncdwp->entrydatasize.nused * SIZEOF_INT);

    /* 
     * Iterate through meta log entries
     */
    headerp = (NC_dw_metadataheader*)ncdwp->metadata.buffer;
    entryp = (NC_dw_metadataentry*)(ncdwp->metadata.buffer + headerp->entry_begin);
    for (lb = 0; lb < ncdwp->metaidx.nused;){
        for (ub = lb; ub < ncdwp->metaidx.nused; ub++) {
            if (ncdwp->metaidx.entries[ub].valid){
                if(ncdwp->entrydatasize.values[ub] + databufferused > databuffersize) {
                    break;  // Buffer full
                }
                else{
                    databufferused += ncdwp->entrydatasize.values[ub]; // Record size of entry
                }
            }
            else{
                // We encounter a canceled record
                // Read unread dat in to data buffer and jump through the gap
                /* 
                 * Read data to buffer
                 * We read only what needed by pending requests
                 */
                if (dataread < databufferused){   
#ifdef PNETCDF_PROFILING
                    t2 = MPI_Wtime();
#endif
                    err = ncdwio_bufferedfile_read(ncdwp->datalog_fd, databuffer + dataread, databufferused - dataread); 
                    if (err != NC_NOERR){
                        return err;
                    }
#ifdef PNETCDF_PROFILING
                    t3 = MPI_Wtime();
                    ncdwp->flush_data_rd_time += t3 - t2;
#endif
                    dataread = databufferused;
                }

                // Skip canceled entry
                err = ncdwio_bufferedfile_seek(ncdwp->datalog_fd, ncdwp->entrydatasize.values[ub], SEEK_CUR);
                if (err != NC_NOERR){
                    return err;
                }
            }
        }

        /* 
         * Read data to buffer
         * We read only what needed by pending requests
         */
        if (dataread < databufferused){   
#ifdef PNETCDF_PROFILING
            t2 = MPI_Wtime();
#endif
            err = ncdwio_bufferedfile_read(ncdwp->datalog_fd, databuffer + dataread, databufferused - dataread); 
            if (err != NC_NOERR){
                return err;
            }
#ifdef PNETCDF_PROFILING
            t3 = MPI_Wtime();
            ncdwp->flush_data_rd_time += t3 - t2;
#endif
            dataread = databufferused;
        }

        /* 
         * If buffer not enough for single entry, we must split it
         * An oversized entry must have triggered a flush on current batch, so entire buffer is free to use
         * We must have entryp points to current entry
         */
        if (ub == lb){
            ip = ncdwp->metaidx.entries + ub;
            
            if (ip->valid){
                /* start, count, stride */
                start = (MPI_Offset*)(entryp + 1);
                count = start + entryp->ndims;
                stride = count + entryp->ndims;
                
                // Convert from log type to MPI type
                err = logtype2mpitype(entryp->itype, &buftype);
                if (err != NC_NOERR){
                    return err;
                }

                /* Determine API_Kind */
                if (entryp->api_kind == NC_LOG_API_KIND_VARA){
                    stride = NULL;    
                }
                           
                /* Replay event in parts */
                err = split_iput(ncdwp, entryp->varid, entryp->ndims, start, count, stride, buftype, entryp->data_off, entryp->data_len, databuffersize, databuffer);
                if (status == NC_NOERR) {
                    status = err;
                }
                
                // Fill up the status for nonblocking request
                if (ip->reqid >= 0){
                    ncdwp->putlist.reqs[ip->reqid].status = err;    
                    ncdwp->putlist.reqs[ip->reqid].ready = 1;  
                }
                           
                /* Update batch status */
                databufferused = 0;
            }
            
            /* Move to next position */
            entryp = (NC_dw_metadataentry*)(((char*)entryp) + entryp->esize);
            
            /* Skip this entry on batch flush */
            lb++;
        }
        else{          
            // Pointer points to the data of current entry
            databufferoff = databuffer;
            
            j = 0;
            for(i = lb; i < ub; i++){
                ip = ncdwp->metaidx.entries + i;

                if (ip->valid) {                   
                    /* start, count, stride */
                    start = (MPI_Offset*)(entryp + 1);
                    count = start + entryp->ndims;
                    stride = count + entryp->ndims;

                    // Convert from log type to MPI type
                    err = logtype2mpitype(entryp->itype, &buftype);
                    if (err != NC_NOERR){
                        return err;
                    }
                    
                    /* Determine API_Kind */
                    if (entryp->api_kind == NC_LOG_API_KIND_VARA){
                        stride = NULL;    
                    }

#ifdef PNETCDF_PROFILING            
                    t2 = MPI_Wtime();
#endif
                    
                    /* Replay event with non-blocking call */
                    err = ncdwp->ncmpio_driver->iput_var(ncdwp->ncp, entryp->varid, start, count, stride, NULL, (void*)(databufferoff), -1, buftype, reqids + j, NC_REQ_WR | NC_REQ_NBI | NC_REQ_HL);
                    if (status == NC_NOERR) {
                        status = err;
                    }

#ifdef PNETCDF_PROFILING                              
                    t3 = MPI_Wtime();
                    ncdwp->flush_put_time += t3 - t2;
#endif

                    // Move to next data location
                    databufferoff += entryp->data_len;
                    j++;
                }
     
                /* Move to next position */
                entryp = (NC_dw_metadataentry*)(((char*)entryp) + entryp->esize);
            }

#ifdef PNETCDF_PROFILING
            t2 = MPI_Wtime();
#endif
            /* 
             * Wait must be called first or previous data will be corrupted
             */
            if (ncdwp->isindep) {
                err = ncdwp->ncmpio_driver->wait(ncdwp->ncp, j, reqids, stats, NC_REQ_INDEP); 
            }
            else{
                err = ncdwp->ncmpio_driver->wait(ncdwp->ncp, j, reqids, stats, NC_REQ_COLL);
            }
            if (status == NC_NOERR) {
                status = err;
            }

#ifdef PNETCDF_PROFILING
            t3 = MPI_Wtime();
            ncdwp->flush_wait_time += t3 - t2;
#endif

            // Fill up the status for nonblocking request
            for(i = lb; i < ub; i++){
                ip = ncdwp->metaidx.entries + i;
                j = 0;
                if (ip->valid) {
                    if (ip->reqid >= 0){
                        ncdwp->putlist.reqs[ip->reqid].status = stats[j];
                        ncdwp->putlist.reqs[ip->reqid].ready = 1;
                    }
                    j++;
                }
            }
          
            /* Update batch status */
            databufferused = 0;

            // Mark as complete
            lb = ub;
        }
    }
               
    /* Free the data buffer */ 
    NCI_Free(databuffer);
    NCI_Free(reqids);
    NCI_Free(stats);

#ifdef PNETCDF_PROFILING
    t4 = MPI_Wtime();
    ncdwp->flush_replay_time += t4 - t1;
#endif

    return status;
}


