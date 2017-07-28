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
#include <stdlib.h>
#include <stdio.h>
/*
 * Create a new log structure
 * IN    comm:    communicator passed to ncmpi_open
 * IN    path:    path of the CDF file
 * IN    bufferdir:    root directory to store log file
 * IN    Parent:    NC structure that will host the log structure
 * OUT    nclogp:    Initialized log structure 
 */
int ncmpii_log_create(NC* ncp) {
    int i, rank, np, err, flag;
    char logbase[NC_LOG_PATH_MAX], basename[NC_LOG_PATH_MAX], hint[NC_LOG_PATH_MAX];
    char *abspath, *fname;
    DIR *logdir;
    size_t ioret, headersize;
    NC_Log_metadataheader *headerp;
    NC_Log *nclogp;
    
    /* Get rank and number of processes */
    err = MPI_Comm_rank(ncp->comm, &rank);
    if (err != MPI_SUCCESS) {
        err = ncmpii_handle_error(err, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(err);
    }
    err = MPI_Comm_size(ncp->comm, &np);
    if (err != MPI_SUCCESS) {
        err = ncmpii_handle_error(err, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(err);
    }
    
    /* Initialize log structure */
 
    /* Allocate the structure */
    nclogp = (NC_Log*)NCI_Malloc(sizeof(NC_Log));
    
    /* Determine log file name
     * Log file name is $(bufferdir)$(basename)_$(ncid)_$(rank).{meta/data}
     * filepath is absolute path to the cdf file
     */

    /* 
     * Make sure bufferdir exists 
     * NOTE: Assume upper layer already check for directory along netcdf file path
     */
    logdir = opendir(ncp->logbase);
    if (logdir == NULL) {
        /* Log base does not exist or not accessible */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    closedir(logdir);

    /* Resolve absolute path */    
    abspath = realpath(ncp->path, basename);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    abspath = realpath(ncp->logbase, logbase);
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
    sprintf(nclogp->metalogpath, "%s%s_%d_%d.meta", logbase, fname, ncp->ncid, rank);
    sprintf(nclogp->datalogpath, "%s%s_%d_%d.data", logbase, fname, ncp->ncid, rank);
     
    /* Initialize metadata buffer */
    err = ncmpii_log_buffer_init(&nclogp->metadata);
    if (err != NC_NOERR){
        return err;
    }
       
    /* Initialize metadata entry array */
    err = ncmpii_log_sizearray_init(&nclogp->entrydatasize);
    if (err != NC_NOERR){
        return err;
    }

    /* Set log file descriptor to NULL */
    nclogp->datalog_fd = -1;
    nclogp->metalog_fd = -1;
 
    /* Misc */
    nclogp->isflushing = 0;   /* Flushing flag, set to 1 when flushing is in progress, 0 otherwise */
    
    /* Initialize metadata header */
    
    /*
     * Allocate space for metadata header
     * Header consists of a fixed size info and variable size basename
     */
    headersize = sizeof(NC_Log_metadataheader) + strlen(basename);
    if (headersize % 4 != 0){
        headersize += 4 - (headersize % 4);
    }
    headerp = (NC_Log_metadataheader*)ncmpii_log_buffer_alloc(&nclogp->metadata, headersize);

    /* Fill up the metadata log header */
    memcpy(headerp->magic, NC_LOG_MAGIC, sizeof(headerp->magic));
    memcpy(headerp->format, NC_LOG_FORMAT_CDF_MAGIC, sizeof(headerp->format));
    strncpy((char*)headerp->basename, basename, headersize - sizeof(NC_Log_metadataheader) + 1);
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
    for(i = 0; i < ncp->vars.ndefined; i++){
        if (ncp->vars.value[i]->ndims > headerp->max_ndims){
            headerp->max_ndims = ncp->vars.value[i]->ndims; 
        }
    }
    headerp->entry_begin = nclogp->metadata.nused;  /* Location of the first entry */
    headerp->basenamelen = strlen(basename);

    /* Create log files */
    flag = O_RDWR | O_CREAT;
    if (!(ncp->loghints & NC_LOG_HINT_LOG_OVERWRITE)) {
        flag |= O_EXCL;
    }
    nclogp->datalog_fd = nclogp->metalog_fd = -1;
    nclogp->metalog_fd = open(nclogp->metalogpath, flag, 0744);
    if (nclogp->metalog_fd < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
    nclogp->datalog_fd = open(nclogp->datalogpath, flag, 0744);
    if (nclogp->datalog_fd < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
   
    /* Write metadata header to file
     * Write from the memory buffer to file
     */
    ioret = write(nclogp->metalog_fd, headerp, headersize);
    if (ioret < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != headersize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Write data header to file 
     * Data header consists of a fixed sized string PnetCDF0
     */
    ioret = write(nclogp->datalog_fd, "PnetCDF0", 8);
    if (ioret < 0){ 
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != 8){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
    nclogp->datalogsize = 8;

    /* attach to nc structure */
    ncp->nclogp = nclogp;

ERROR:;
    if (err != NC_NOERR) {
        NCI_Free(nclogp);
    }
    
    return NC_NOERR;
}

/*
 * Create log file for the log structure
 * IN    nclogp:    log structure
 */
int ncmpii_log_enddef(NC *ncp){   
    int i, maxdims, err;
    ssize_t ioret;
    NC_Log *nclogp = ncp->nclogp;
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
            DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
        }
        /* Overwrite num_entries
         * This marks the completion of the record
         */
        ioret = write(nclogp->metalog_fd, &headerp->max_ndims, SIZEOF_MPI_OFFSET);
        if (ioret < 0){
            err = ncmpii_handle_io_error("write");
            if (err == NC_EFILE){
                err = NC_EWRITE;
            }
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != SIZEOF_MPI_OFFSET){
            err = ncmpii_handle_io_error("write");
            
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
    }

    return NC_NOERR;
}

/*
 * Flush the log to CDF file and clean up the log structure 
 * Used by ncmpi_close()
 * IN    nclogp:    log structure
 */
int ncmpii_log_close(NC *ncp) {
    int err;
    NC_Log *nclogp = ncp->nclogp;
    NC_Log_metadataheader* headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;

    /* If log file is created, flush the log */
    if (nclogp->metalog_fd >= 0){
        /* Commit to CDF file */
        if (headerp->num_entries > 0){
            ncp->logflushing = 1;
            log_flush(ncp);
            ncp->logflushing = 0;
        }

        /* Close log file */
        err = close(nclogp->metalog_fd);
        err |= close(nclogp->datalog_fd);
        if (err < 0){
            DEBUG_RETURN_ERROR(ncmpii_handle_io_error("close"));        
        }

        /* Delete log files if delete flag is set */
        if (ncp->loghints & NC_LOG_HINT_DEL_ON_CLOSE){
            unlink(nclogp->datalogpath);
            unlink(nclogp->metalogpath);
        }
    }

    /* Free meta data buffer and metadata offset list*/
    ncmpii_log_buffer_free(&nclogp->metadata);
    ncmpii_log_sizearray_free(&nclogp->entrydatasize);

    /* Delete log structure */
    NCI_Free(nclogp);

    return NC_NOERR;
}

/*
 * Prepare a single log entry to be write to log
 * Used by ncmpii_getput_varm
 * IN    nclogp:    log structure to log this entry
 * IN    varp:    NC_var structure associate to this entry
 * IN    start: start in put_var* call
 * IN    count: count in put_var* call
 * IN    stride: stride in put_var* call
 * IN    bur:    buffer of data to write
 * IN    buftype:    buftype as in ncmpii_getput_varm, MPI_PACKED indicate a flexible api
 * IN    PackedSize:    Size of buf in byte, only valid when buftype is MPI_PACKED
 */
int ncmpii_log_put_var(NC *ncp, NC_var *varp, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Datatype buftype, int PackedSize){
    int err, varid, dim;
    int itype;    /* Type used in log file */
    char *buffer;
    MPI_Offset esize, dataoff;
    MPI_Offset *Start, *Count, *Stride;
    ssize_t ioret;
    NC_Log_metadataentry *entryp;
    NC_Log_metadataheader *headerp;
    NC_Log *nclogp = ncp->nclogp;

    /* Enddef must be called at least once */
    if (nclogp->metalog_fd < 0){
        DEBUG_RETURN_ERROR(NC_ELOGNOTINIT);        
    }

    /* Get variable id and dimension */
    dim = varp->ndims;
    varid = varp->varid;

    /* Convert to log types 
     * Log spec has different enum of types than MPI
     */
    switch (buftype) {
    case MPI_CHAR:    /* put_*_text */
        itype = NC_LOG_TYPE_TEXT;
        break;
    case MPI_SIGNED_CHAR:    /* put_*_schar */
        itype = NC_LOG_TYPE_SCHAR;
        break;
    case MPI_UNSIGNED_CHAR:    /* put_*_uchar */
        itype = NC_LOG_TYPE_UCHAR;
        break;
    case MPI_UNSIGNED_SHORT: /* put_*_ushort */
        itype = NC_LOG_TYPE_USHORT;
        break;
    case MPI_INT: /* put_*_int */
        itype = NC_LOG_TYPE_INT;
        break;
    case MPI_UNSIGNED: /* put_*_uint */
        itype = NC_LOG_TYPE_UINT;
        break;
    case MPI_FLOAT: /* put_*_float */
        itype = NC_LOG_TYPE_FLOAT;
        break;
    case MPI_DOUBLE: /* put_*_double */
        itype = NC_LOG_TYPE_DOUBLE;
        break;
    case MPI_LONG_LONG_INT: /* put_*_longlong */
        itype = NC_LOG_TYPE_LONGLONG;
        break;
    case MPI_UNSIGNED_LONG_LONG: /* put_*_ulonglong */
        itype = NC_LOG_TYPE_ULONGLONG;
        break;
    default: /* Unrecognized type */
        DEBUG_RETURN_ERROR(NC_EINVAL);
        break;
    }
    
    /* Writing to data log
     * Note: Metadata record indicate completion, so data must go first 
     */
    
    /* Find out the location of data in datalog
     * Which is current possition in data log 
     * Datalog descriptor should always points to the end of file
     * Position must be recorded first before writing
     */
    dataoff = (MPI_Offset)nclogp->datalogsize;
    
    /* 
     * Write data log
     * We only increase datalogsize by amount actually write
     */
    ioret = write(nclogp->datalog_fd, buf, PackedSize);
    if (ioret < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    nclogp->datalogsize += ioret;
    if (ioret != PackedSize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
 
    /* Prepare metadata entry header */
    
    /* Record offset 
     * Note: metadata will be updated after allocating metadata buffer space, seek must be done first 
     */
    
    /* Seek to the head of metadata
     * Note: EOF may not be the place for next entry after a flush
     * Note: metadata size will be updated after allocating metadata buffer space, seek must be done first 
     */
    ioret = lseek(nclogp->metalog_fd, nclogp->metadata.nused, SEEK_SET);   
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }

    /* Size of metadata entry
     * Include metadata entry header and variable size additional data (start, count, stride) 
     */
    esize = sizeof(NC_Log_metadataentry) + dim * 3 * SIZEOF_MPI_OFFSET;
    
    /* Allocate space for metadata entry header */
    buffer = (char*)ncmpii_log_buffer_alloc(&nclogp->metadata, esize);
    entryp = (NC_Log_metadataentry*)buffer;
    entryp->esize = esize; /* Entry size */
    entryp->itype = itype; /* Variable type */
    entryp->varid = varid;  /* Variable id */
    entryp->ndims = dim;  /* Number of dimensions of the variable*/
	entryp->data_len = PackedSize; /* The size of data in bytes. The size that will be write to data log */

    /* Find out the location of data in datalog
     * Which is current possition in data log 
     * Datalog descriptor should always points to the end of file
     */
    ioret = lseek(nclogp->datalog_fd, 0, SEEK_CUR);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
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
    Start = (MPI_Offset*)(buffer + sizeof(NC_Log_metadataentry));
    Count = Start + dim;
    Stride = Count + dim;
    
    /* Fill up start, count, and stride */
    memcpy(Start, start, dim * SIZEOF_MPI_OFFSET);
    memcpy(Count, count, dim * SIZEOF_MPI_OFFSET);
    if(stride != NULL){
        memcpy(Stride, stride, dim * SIZEOF_MPI_OFFSET);
    }
    /* Write meta data log */
    ioret = write(nclogp->metalog_fd, buffer, esize);
    if (ioret < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != esize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Increase number of entry
     * This must be the final step of a log record
     * Increasing num_entries marks the completion of the record
     */

    /* Increase num_entries in the metadata buffer */
    headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
    headerp->num_entries++;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(nclogp->metalog_fd, 56, SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of the record
     */
    ioret = write(nclogp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ioret < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != SIZEOF_MPI_OFFSET){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Record data size */
    ncmpii_log_sizearray_append(&nclogp->entrydatasize, entryp->data_len);
    
    return NC_NOERR;
}

/*
 * Commit the log into cdf file and delete the log
 * User can call this to force a commit without closing
 * It work by flush and re-initialize the log structure
 * IN    nclogp:    log structure
 */
int ncmpii_log_flush(NC* ncp) {
    int err;
    size_t ioret;
    NC_Log *nclogp = ncp->nclogp;
    NC_Log_metadataheader *headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;

    /* Nothing to replay if nothing have been written */
    if (headerp->num_entries == 0){
        return NC_NOERR;
    }
    
    /* Replay log file */
    ncp->logflushing = 1;
    if ((err = log_flush(ncp)) != NC_NOERR) {
        ncp->logflushing = 0;
        return err;
    }
    ncp->logflushing = 0;
    
    /* Reset log status */
    
    /* Set num_entries to 0 */
    headerp->num_entries = 0;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(nclogp->metalog_fd, 56, SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of flush
     */
    ioret = write(nclogp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ioret < 0){
        err = ncmpii_handle_io_error("write");
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
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    nclogp->datalogsize = 8;

    return NC_NOERR;
}

