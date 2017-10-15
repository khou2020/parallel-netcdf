#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
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
#include <stdlib.h>
#include <stdio.h>
#include <ncbbio_driver.h>

/*
 * Create a new log structure
 * IN      info:    MPI info passed to ncmpi_create/ncmpi_open
 * INOUT   ncbbp:   NC_bb object holding the log structure 
 */
int ncbbio_log_create(NC_bb* ncbbp, MPI_Info info) {
    int i, rank, np, err, flag;
    char logbase[NC_LOG_PATH_MAX], basename[NC_LOG_PATH_MAX];
    char hint[NC_LOG_PATH_MAX], value[MPI_MAX_INFO_VAL];
    char *abspath, *fname;
    double t1, t2;
    DIR *logdir;
    size_t ioret, headersize;
    NC_bb_metadataheader *headerp;
    
    t1 = MPI_Wtime();
    
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

    /* Get rank and number of processes */
    err = MPI_Comm_rank(ncbbp->comm, &rank);
    if (err != MPI_SUCCESS) {
        err = ncmpii_error_mpi2nc(err, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(err);
    }
    err = MPI_Comm_size(ncbbp->comm, &np);
    if (err != MPI_SUCCESS) {
        err = ncmpii_error_mpi2nc(err, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(err);
    }
    
    /* Initialize log structure */
    
    /* Determine log file name
     * Log file name is $(bufferdir)$(basename)_$(ncid)_$(rank).{meta/data}
     * filepath is absolute path to the cdf file
     */

    /* 
     * Make sure bufferdir exists 
     * NOTE: Assume directory along netcdf file path exists
     */
    logdir = opendir(ncbbp->logbase);
    if (logdir == NULL) {
        /* Log base does not exist or not accessible */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    closedir(logdir);

    /* Resolve absolute path */    
    abspath = realpath(ncbbp->path, basename);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    abspath = realpath(ncbbp->logbase, logbase);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }

    /* Extract file anme 
     * Search for first / charactor from the tail
     * Absolute path should always contains one '/', return error otherwise
     * We return the string including '/' for convenience
     */
    fname = strrchr(basename, '/');
    if (fname == NULL){
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);  
    }
    
    /* Log file path may also contain non-existing directory
     * We need to create them before we can search for usable id
     * As log file name hasn't been determined, we need to use a dummy one here
     */
    sprintf(ncbbp->metalogpath, "%s%s_%d_%d.meta", logbase, fname, ncbbp->ncid, rank);
    sprintf(ncbbp->datalogpath, "%s%s_%d_%d.data", logbase, fname, ncbbp->ncid, rank);
     
    /* Initialize metadata buffer */
    err = ncbbio_log_buffer_init(&ncbbp->metadata);
    if (err != NC_NOERR){
        return err;
    }
       
    /* Initialize metadata entry array */
    err = ncbbio_log_sizearray_init(&ncbbp->entrydatasize);
    if (err != NC_NOERR){
        return err;
    }

    /* Set log file descriptor to NULL */
    ncbbp->datalog_fd = -1;
    ncbbp->metalog_fd = -1;

    /* Performance counters */
    ncbbp->total_data = 0;
    ncbbp->total_meta = 0;
    ncbbp->flush_read_time = 0;
    ncbbp->flush_replay_time = 0;
    ncbbp->flush_total_time = 0;
    ncbbp->log_write_time = 0;
    ncbbp->log_total_time = 0;
    ncbbp->total_time = 0;

    /* Misc */
    ncbbp->rank = rank;
    ncbbp->np = np;

    /* Initialize metadata header */
    
    /*
     * Allocate space for metadata header
     * Header consists of a fixed size info and variable size basename
     */
    headersize = sizeof(NC_bb_metadataheader) + strlen(basename);
    if (headersize % 4 != 0){
        headersize += 4 - (headersize % 4);
    }
    headerp = (NC_bb_metadataheader*)ncbbio_log_buffer_alloc(&ncbbp->metadata, headersize);

    /* Fill up the metadata log header */
    memcpy(headerp->magic, NC_LOG_MAGIC, sizeof(headerp->magic));
    memcpy(headerp->format, NC_LOG_FORMAT_CDF_MAGIC, sizeof(headerp->format));
    strncpy((char*)headerp->basename, basename, headersize - sizeof(NC_bb_metadataheader) + 1);
    headerp->rank_id = rank;   /* Rank */
    headerp->num_ranks = np;   /* Number of processes */
    headerp->is_external = 0;    /* Without convertion before logging, data in native representation */
    /* Determine endianess */
#ifdef WORDS_BIGENDIAN 
    headerp->big_endian = NC_LOG_TRUE;
#else 
    headerp->big_endian = NC_LOG_FALSE;
#endif
    headerp->num_entries = 0;    /* The log is empty */
    /* Highest dimension among all variables */
    headerp->max_ndims = 0;    
    headerp->entry_begin = ncbbp->metadata.nused;  /* Location of the first entry */
    headerp->basenamelen = strlen(basename);

    /* Create log files */
    flag = O_RDWR | O_CREAT;
    if (!(ncbbp->hints & NC_LOG_HINT_LOG_OVERWRITE)) {
        flag |= O_EXCL;
    }
    ncbbp->datalog_fd = ncbbp->metalog_fd = -1;
    ncbbp->metalog_fd = open(ncbbp->metalogpath, flag, 0744);
    if (ncbbp->metalog_fd < 0) {
        err = ncmpii_error_posix2nc("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
    ncbbp->datalog_fd = open(ncbbp->datalogpath, flag, 0744);
    if (ncbbp->datalog_fd < 0) {
        err = ncmpii_error_posix2nc("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
   
    /* Write metadata header to file
     * Write from the memory buffer to file
     */
    ioret = write(ncbbp->metalog_fd, headerp, headersize);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != headersize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Write data header to file 
     * Data header consists of a fixed sized string PnetCDF0
     */
    ioret = write(ncbbp->datalog_fd, "PnetCDF0", 8);
    if (ioret < 0){ 
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != 8){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
    ncbbp->datalogsize = 8;
   
ERROR:;
    
    t2 = MPI_Wtime();
    ncbbp->total_time += t2 - t1;

    ncbbp->total_meta += headersize;
    ncbbp->total_data += 8;

    return NC_NOERR;
}

/*
 * Update information used by bb layer on enddef
 * IN    ncbbp:    NC_bb structure
 */
int ncbbio_log_enddef(NC_bb *ncbbp){   
    int i, maxdims, err, recdimid;
    ssize_t ioret;
    double t1, t2; 
    NC_bb_metadataheader *headerp = (NC_bb_metadataheader*)ncbbp->metadata.buffer;
    
    t1 = MPI_Wtime();
    
    /* 
     * Update the header if max ndim increased 
     * For now, max_ndims is the only field that can change
     */
    if (ncbbp->max_ndims > headerp->max_ndims){
        headerp->max_ndims = ncbbp->max_ndims;

        /* Seek to the location of maxndims 
         * Note: location need to be updated when struct change
         */
        ioret = lseek(ncbbp->metalog_fd, sizeof(NC_bb_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->basenamelen) - sizeof(headerp->num_entries) - sizeof(headerp->max_ndims), SEEK_SET);
        if (ioret < 0){
            DEBUG_RETURN_ERROR(ncmpii_error_posix2nc("lseek"));
        }
        /* Overwrite maxndims
         * This marks the completion of the record
         */
        ioret = write(ncbbp->metalog_fd, &headerp->max_ndims, SIZEOF_MPI_OFFSET);
        if (ioret < 0){
            err = ncmpii_error_posix2nc("write");
            if (err == NC_EFILE){
                err = NC_EWRITE;
            }
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != SIZEOF_MPI_OFFSET){
            err = ncmpii_error_posix2nc("write");
            
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
    }
    
    t2 = MPI_Wtime();
    ncbbp->total_time += t2 - t1;

    return NC_NOERR;
}

/*
 * Flush the log to CDF file and clean up the log structure 
 * Used by ncmpi_close()
 * IN    ncbbp:    log structure
 */
int ncbbio_log_close(NC_bb *ncbbp) {
    int err;
    double t1, t2; 
    unsigned long long total_data;
    unsigned long long total_meta;
    double flush_read_time;
    double flush_replay_time;
    double flush_total_time;
    double log_write_time;
    double log_total_time;
    double total_time;
    NC_bb_metadataheader* headerp = (NC_bb_metadataheader*)ncbbp->metadata.buffer;

    t1 = MPI_Wtime();

    /* If log file is created, flush the log */
    if (ncbbp->metalog_fd >= 0){
        /* Commit to CDF file */
        if (headerp->num_entries > 0){
            log_flush(ncbbp);
        }

        /* Close log file */
        err = close(ncbbp->metalog_fd);
        err |= close(ncbbp->datalog_fd);
        if (err < 0){
            DEBUG_RETURN_ERROR(ncmpii_error_posix2nc("close"));        
        }

        /* Delete log files if delete flag is set */
        if (ncbbp->hints & NC_LOG_HINT_DEL_ON_CLOSE){
            unlink(ncbbp->datalogpath);
            unlink(ncbbp->metalogpath);
        }
    }

    /* Free meta data buffer and metadata offset list*/
    ncbbio_log_buffer_free(&ncbbp->metadata);
    ncbbio_log_sizearray_free(&ncbbp->entrydatasize);
    
    t2 = MPI_Wtime();
    ncbbp->total_time += t2 - t1;

    /* Print accounting info in debug build */
#ifdef PNETCDF_DEBUG
    MPI_Reduce(&ncbbp->total_time, &total_time, 1, MPI_DOUBLE, MPI_MAX, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->flush_read_time, &flush_read_time, 1, MPI_DOUBLE, MPI_MAX, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->flush_replay_time, &flush_replay_time, 1, MPI_DOUBLE, MPI_MAX, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->flush_total_time, &flush_total_time, 1, MPI_DOUBLE, MPI_MAX, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->log_write_time, &log_write_time, 1, MPI_DOUBLE, MPI_MAX, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->log_total_time, &log_total_time, 1, MPI_DOUBLE, MPI_MAX, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->total_meta, &total_meta, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, ncbbp->comm);
    MPI_Reduce(&ncbbp->total_data, &total_data, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, ncbbp->comm);
    
    if (ncbbp->rank == 0){ 
        printf("==========================================================\n");
        printf("File: %s\n", ncbbp->path);
        printf("Data writen to variable: %llu\n", total_data);
        printf("Metadata generated: %llu\n", total_meta);
        printf("Flush buffer size: %llu\n", ncbbp->logflushbuffersize);
        printf("Time in log: %lf\n", total_time);
        printf("\tTime recording entries: %lf\n", log_total_time);
        printf("\t\tTime writing to BB: %lf\n", log_write_time);
        printf("\tTime flushing: %lf\n", flush_total_time);
        printf("\t\tTime reading from BB: %lf\n", flush_read_time);
        printf("\t\tTime replaying: %lf\n", flush_replay_time);
        printf("==========================================================\n");
    }
#endif

    return NC_NOERR;
}

/*
 * Prepare a single log entry to be write to log
 * Used by ncmpii_getput_varm
 * IN    ncbbp:    log structure to log this entry
 * IN    varp:    NC_var structure associate to this entry
 * IN    start: start in put_var* call
 * IN    count: count in put_var* call
 * IN    stride: stride in put_var* call
 * IN    bur:    buffer of data to write
 * IN    buftype:    buftype as in ncmpii_getput_varm, MPI_PACKED indicate a flexible api
 * IN    packedsize:    Size of buf in byte, only valid when buftype is MPI_PACKED
 */
int ncbbio_log_put_var(NC_bb *ncbbp, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Datatype buftype, MPI_Offset *putsize){
    int err, i, dim, elsize;
    int itype;    /* Type used in log file */
    int *dimids;
    char *buffer;
    double t1, t2, t3, t4; 
    MPI_Offset esize, dataoff, recsize;
    MPI_Offset *Start, *Count, *Stride;
    MPI_Offset size;
    ssize_t ioret;
    NC_bb_metadataentry *entryp;
    NC_bb_metadataheader *headerp;
    
    t1 = MPI_Wtime();

    /* Calculate data size */
    /* Get ndims */
    err = ncbbp->ncmpio_driver->inq_var(ncbbp->ncp, varid, NULL, NULL, &dim, 
                                        NULL, NULL, NULL, NULL, NULL);
    if (err != NC_NOERR){
        return err;
    }
    /* Calcalate submatrix size */
    MPI_Type_size(buftype, &elsize);
    size = (MPI_Offset)elsize;
    for(i = 0; i < dim; i++){
        size *= count[i];
    }

    /* Update record dimension size if is record variable */
    /* Get dimids */
    dimids = NCI_Malloc(sizeof(int) * dim);
    err = ncbbp->ncmpio_driver->inq_var(ncbbp->ncp, varid, NULL, NULL, NULL, 
                                        dimids, NULL, NULL, NULL, NULL);
    if (err != NC_NOERR){
        return err;
    }
    /* Update recdimsize if first dim is unlimited */
    if (dim > 0 && dimids[0] == ncbbp->recdimid) {
        /* Dim size after the put operation */
        if (stride == NULL) {
            recsize = start[0] + count[0];
        }
        else {
            recsize = start[0] + (count[0] - 1) * stride[0] + 1;
        }
        if (recsize > ncbbp->recdimsize) {
            ncbbp->recdimsize = recsize;
        }
    }
    NCI_Free(dimids);

    /* Convert to log types 
     * Log spec has different enum of types than MPI
     */
    if (buftype == MPI_CHAR) {   /* put_*_text */
        itype = NC_LOG_TYPE_TEXT;
    }
    else if (buftype == MPI_SIGNED_CHAR) {    /* put_*_schar */
        itype = NC_LOG_TYPE_SCHAR;
    }
    else if (buftype == MPI_UNSIGNED_CHAR) {    /* put_*_uchar */
        itype = NC_LOG_TYPE_UCHAR;
    }
    else if (buftype == MPI_SHORT) { /* put_*_ushort */
        itype = NC_LOG_TYPE_SHORT;
    }
    else if (buftype == MPI_UNSIGNED_SHORT) { /* put_*_ushort */
        itype = NC_LOG_TYPE_USHORT;
    }
    else if (buftype == MPI_INT) { /* put_*_int */
        itype = NC_LOG_TYPE_INT;
    }
    else if (buftype == MPI_UNSIGNED) { /* put_*_uint */
        itype = NC_LOG_TYPE_UINT;
    }
    else if (buftype == MPI_FLOAT) { /* put_*_float */
        itype = NC_LOG_TYPE_FLOAT;
    }
    else if (buftype == MPI_DOUBLE) { /* put_*_double */
        itype = NC_LOG_TYPE_DOUBLE;
    }
    else if (buftype == MPI_LONG_LONG_INT) { /* put_*_longlong */
        itype = NC_LOG_TYPE_LONGLONG;
    }
    else if (buftype == MPI_UNSIGNED_LONG_LONG) { /* put_*_ulonglong */
        itype = NC_LOG_TYPE_ULONGLONG;
    }
    else { /* Unrecognized type */
        DEBUG_RETURN_ERROR(NC_EINVAL);
    }
    
    /* Prepare metadata entry header */
        
    /* Find out the location of data in datalog
     * Which is current possition in data log 
     * Datalog descriptor should always points to the end of file
     * Position must be recorded first before writing
     */
    dataoff = (MPI_Offset)ncbbp->datalogsize;
    
    /* Size of metadata entry
     * Include metadata entry header and variable size additional data (start, count, stride) 
     */
    esize = sizeof(NC_bb_metadataentry) + dim * 3 * SIZEOF_MPI_OFFSET;
    /* Allocate space for metadata entry header */
    buffer = (char*)ncbbio_log_buffer_alloc(&ncbbp->metadata, esize);
    entryp = (NC_bb_metadataentry*)buffer;
    entryp->esize = esize; /* Entry size */
    entryp->itype = itype; /* Variable type */
    entryp->varid = varid;  /* Variable id */
    entryp->ndims = dim;  /* Number of dimensions of the variable*/
	entryp->data_len = size; /* The size of data in bytes. The size that will be write to data log */
    entryp->data_off = dataoff;
    
    /* Determine the api kind of original call
     * If stride is NULL, we log it as a vara call, otherwise, a vars call 
     * Upper layer translates var1 and var to vara, so we only have vara and vars
     */
    if (stride == NULL){
        entryp->api_kind = NC_LOG_API_KIND_VARA;
    }
    else{
        entryp->api_kind = NC_LOG_API_KIND_VARS;
    }
    
    /* Calculate location of start, count, stride in metadata buffer */
    Start = (MPI_Offset*)(buffer + sizeof(NC_bb_metadataentry));
    Count = Start + dim;
    Stride = Count + dim;
    
    /* Fill up start, count, and stride */
    memcpy(Start, start, dim * SIZEOF_MPI_OFFSET);
    memcpy(Count, count, dim * SIZEOF_MPI_OFFSET);
    if(stride != NULL){
        memcpy(Stride, stride, dim * SIZEOF_MPI_OFFSET);
    }
 
    t2 = MPI_Wtime();
    
    /* Writing to data log
     * Note: Metadata record indicate completion, so data must go first 
     */

    /* 
     * Write data log
     * We only increase datalogsize by amount actually write
     */
    ioret = write(ncbbp->datalog_fd, buf, size);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    ncbbp->datalogsize += ioret;
    if (ioret != size){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
       
    /* Seek to the head of metadata
     * Note: EOF may not be the place for next entry after a flush
     * Note: metadata size will be updated after allocating metadata buffer space, substract esize for original location 
     */
    ioret = lseek(ncbbp->metalog_fd, ncbbp->metadata.nused - esize, SEEK_SET);   
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_error_posix2nc("lseek"));
    }
    
    /* Write meta data log */
    ioret = write(ncbbp->metalog_fd, buffer, esize);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != esize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
    t3 = MPI_Wtime();

    /* Increase number of entry
     * This must be the final step of a log record
     * Increasing num_entries marks the completion of the record
     */

    /* Increase num_entries in the metadata buffer */
    headerp = (NC_bb_metadataheader*)ncbbp->metadata.buffer;
    headerp->num_entries++;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(ncbbp->metalog_fd, 56, SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_error_posix2nc("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of the record
     */
    ioret = write(ncbbp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != SIZEOF_MPI_OFFSET){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Record data size */
    ncbbio_log_sizearray_append(&ncbbp->entrydatasize, entryp->data_len);
    
    t4 = MPI_Wtime();
    ncbbp->log_total_time += t4 - t1;
    ncbbp->log_write_time += t3 - t2;
    ncbbp->total_time += t4 - t1;
 
    ncbbp->total_data += size;
    ncbbp->total_meta += esize;

    /* Return size */
    if (putsize != NULL){
        *putsize = size;
    }

    return NC_NOERR;
}

/*
 * Commit the log into cdf file and delete the log
 * User can call this to force a commit without closing
 * It work by flush and re-initialize the log structure
 * IN    ncbbp:    log structure
 */
int ncbbio_log_flush(NC_bb* ncbbp) {
    int err, status = NC_NOERR;
    int numput;
    double t1, t2; 
    size_t ioret;
    //NC_req *putlist;
    NC_bb_metadataheader *headerp = (NC_bb_metadataheader*)ncbbp->metadata.buffer;
    
    t1 = MPI_Wtime();

    /* Nothing to replay if nothing have been written */
    if (headerp->num_entries == 0){
        return NC_NOERR;
    }

    /* Replay log file */
    err = log_flush(ncbbp);
    if (err != NC_NOERR) {
        if (status == NC_NOERR){
            DEBUG_ASSIGN_ERROR(status, err);
        }
    }
 
    /* Reset log status */
    
    /* Set num_entries to 0 */
    headerp->num_entries = 0;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(ncbbp->metalog_fd, 56, SEEK_SET);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("lseek");
        if (status == NC_NOERR){
            DEBUG_ASSIGN_ERROR(status, err);
        }
    }
    /* Overwrite num_entries
     * This marks the completion of flush
     */
    ioret = write(ncbbp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        if (status == NC_NOERR){
            DEBUG_ASSIGN_ERROR(status, err);
        }
    }
    if (ioret != SIZEOF_MPI_OFFSET){
        if (status == NC_NOERR){
            DEBUG_ASSIGN_ERROR(status, err);
        }
    }

    /* Reset metadata buffer and entry array status */
    ncbbp->metadata.nused = headerp->entry_begin;
    ncbbp->entrydatasize.nused = 0;

    /* Rewind data log file descriptors and reset the size */
    ioret = lseek(ncbbp->datalog_fd, 8, SEEK_SET);
    if (ioret != 8){
        err = ncmpii_error_posix2nc("lseek");
        if (status == NC_NOERR){
            DEBUG_ASSIGN_ERROR(status, err);
        }
    }
    ncbbp->datalogsize = 8;
   
    t2 = MPI_Wtime();
    ncbbp->total_time += t2 - t1;

    return status;
}