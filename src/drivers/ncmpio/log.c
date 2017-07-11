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

#define LOG_BUFFER_SIZE 1024 /* Size of initial metadata buffer */
#define LOG_ARRAY_SIZE 32 /* Size of initial metadata offset list */    
#define SIZE_MULTIPLIER 20    /* When metadata buffer is full, we'll NCI_Reallocate it to META_BUFFER_MULTIPLIER times the original size*/

/* 
 * Initialize a variable sized buffer
 * IN   bp: buffer structure to be initialized
 */
int ncmpii_log_buffer_init(NC_Log_buffer * bp){
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
void ncmpii_log_buffer_free(NC_Log_buffer * bp){
    NCI_Free(bp->buffer);
}

/*
 * Allocate space in the variable sized buffer
 * This function works as NCI_Malloc in the metadata buffer
 * IN    bp:    buffer structure
 * IN    size:    size required in the buffer
 */
char* ncmpii_log_buffer_alloc(NC_Log_buffer *bp, size_t size) {
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
        /* ret is used to temporaryly hold the allocated buffer so we don't lose nclogp->metadata.buffer if allocation fails */
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
int ncmpii_log_sizearray_init(NC_Log_sizearray *sp){
    sp->values = NCI_Malloc(LOG_ARRAY_SIZE * sizeof(NC_Log_metadataentry*));
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
void ncmpii_log_sizearray_free(NC_Log_sizearray *sp){
    NCI_Free(sp->values);
}

/*
 * Append entry to array
 * IN    ep:    array structure
 * IN    ent:    entry to be added
 */
int ncmpii_log_sizearray_append(NC_Log_sizearray *sp, size_t size) {
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
        /* ret is used to temporaryly hold the allocated buffer so we don't lose nclogp->metadata.buffer if allocation fails */
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

/*
 * Create directories along the log file path
 * open does not create directory along the path is it does not exists, so we must create it before open
 * Code is copied from internet
 * IN    file_path:    path of the file
 * IN    mode: file permission mode of the newly created firectory    
 * IN    isdir: if file_path is a directory, we need to create the directly for final directory even if it don't end with '/'    
 */
int mkpath(char* file_path, mode_t mode, int isdir) {
    char *p;
    
    /* Call mkdir for each '/' encountered along the path
     * The first charactor is skipped, preventing calling on /
     */
    for (p = strchr(file_path + 1, '/'); p; p = strchr(p + 1, '/')) {
        *p = '\0';
        if (mkdir(file_path, mode) == -1) {
            if (errno != EEXIST) { 
                *p = '/';
                return -1;
            }
        }
        *p = '/';
    }
    
    /* If target is itself a directory, we must create it even if it don't end with '/' */
    if (isdir) {
        return mkdir(file_path, mode);
    }
    
    return 0;
}

/*
 * Check if running on a little endian machine
 */
int IsBigEndian() {
    unsigned int n = 1;
    char* b1 = (char*)&n;
    return (int)(*b1) != 1;
}

/*
 * Create a new log structure
 * IN    comm:    communicator passed to ncmpi_open
 * IN    path:    path of the CDF file
 * IN    bufferdir:    root directory to store log file
 * IN    Parent:    NC structure that will host the log structure
 * OUT    nclogp:    Initialized log structure 
 */
int ncmpii_log_create(NC* ncp) {
    int rank, np, err;
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
    memset(nclogp->filepath, 0, sizeof(nclogp->filepath));
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
    strncpy(headerp->basename, nclogp->filepath, headersize - sizeof(NC_Log_metadataheader) + 1);
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
    headerp->max_ndims = 0;    /* Highest dimension among all variables, not used */
    headerp->entry_begin = nclogp->metadata.nused;  /* Location of the first entry */
    headerp->basenamelen = strlen(basename);

    /* Create log files */
    
    nclogp->datalog_fd = nclogp->metalog_fd = -1;
    nclogp->metalog_fd = open(nclogp->metalogpath, O_RDWR | O_CREAT | O_EXCL, 0744);
    if (nclogp->metalog_fd < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
    nclogp->datalog_fd = open(nclogp->datalogpath, O_RDWR | O_CREAT | O_EXCL, 0744);
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
    return NC_NOERR;
}

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    nclogp:    log structure
 */
int log_flush(NC *ncp) {
    int i, j, err, fd;
    size_t databatchsize, databuffersize, databufferused, databufferidx;
    ssize_t ioret;
    NC_Log_metadataentry *entryp;
    NC_var *varp;
    MPI_Offset *start, *count, *stride;
    MPI_Datatype buftype;
    char *databuffer;
    NC_Log_metadataheader* headerp;
    NC_Log *nclogp = ncp->nclogp;

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
    if (ncp->logflushbuffersize > 0 && databuffersize > ncp->logflushbuffersize){
        databuffersize = ncp->logflushbuffersize;
    }
    /* Allocate buffer */
    databuffer = (char*)NCI_Malloc(databuffersize);
    if(databuffer == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }

    /* Seek to the start position of first data record */
    ioret = lseek(nclogp->datalog_fd, 8, SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Initialize buffer status */
    databufferidx = 8;
    databufferused = 0;
    databatchsize = 0;

    /* 
     * Iterate through meta log entries
     * i is entries scaned for size
     * j is entries replayed
     */
    headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
    entryp = (NC_Log_metadataentry*)(nclogp->metadata.buffer + headerp->entry_begin);
    for (i = j = 0; i < nclogp->entrydatasize.nused; i++){
        
        /* Return error if buffer not enough for single entry */
        if (nclogp->entrydatasize.values[i] > databuffersize){
            return NC_ENOMEM;
        }
        
        /* Process current batch if data buffer can not accomodate next one */
        if(databatchsize + nclogp->entrydatasize.values[i] >= databuffersize){
#ifdef PNETCDF_DEBUG
            tbegin = MPI_Wtime();
#endif
            /* 
             * Read data to buffer
             * We read only what needed by pending requests
             */
            ioret = read(nclogp->datalog_fd, databuffer, databatchsize); 
            if (ioret < 0) {
                ioret = ncmpii_handle_io_error("read");
                if (ioret == NC_EFILE){
                    ioret = NC_EREAD;
                }
                DEBUG_RETURN_ERROR(ioret);
            }
            if (ioret != databatchsize){
                DEBUG_RETURN_ERROR(NC_EBADLOG);
            }

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            tread += tend - tbegin;
            tbegin = MPI_Wtime();
#endif
            for(; j < i; j++){
                // entryp = (NC_Log_metadataentry*)head;
                
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
                    stride = NULL;    
                }

                /* Play event */
                
                /* Translate varid to varp */
                err = ncmpii_NC_lookupvar(ncp, entryp->varid, &varp);
                if (err != NC_NOERR){
                    return err;
                }
                /* Replay event with non-blocking call */
                err = ncmpii_igetput_varm(ncp, varp, start, count, stride, NULL, (void*)(databuffer + entryp->data_off - databufferidx), -1, buftype, NULL, WRITE_REQ, 0, 0);
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
             * Collective wait
             * Wait must be called first or previous data will be corrupted
             */
            err = ncmpii_wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
            if (err != NC_NOERR) {
                return err;
            }

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            twait += tend - tbegin;
#endif
            /* Update batch status */
            databufferidx += databatchsize;
            databatchsize = 0;
        }
        
        /* Record current entry size */
        databatchsize += nclogp->entrydatasize.values[i];
    }
    
    /* Process last batch if there are unflushed entries */
    if(databatchsize > 0){
#ifdef PNETCDF_DEBUG
        tbegin = MPI_Wtime();
#endif
        /* 
         * Read data to buffer
         * We read only what needed by pending requests
         */
        ioret = read(nclogp->datalog_fd, databuffer, databatchsize); 
        if (ioret < 0) {
            ioret = ncmpii_handle_io_error("read");
            if (ioret == NC_EFILE){
                ioret = NC_EREAD;
            }
            DEBUG_RETURN_ERROR(ioret);
        }
        if (ioret != databatchsize){
            DEBUG_RETURN_ERROR(NC_EBADLOG);
        }

#ifdef PNETCDF_DEBUG
        tend = MPI_Wtime();
        tread += tend - tbegin;
        tbegin = MPI_Wtime();
#endif
        for(; j < i; j++){
            // entryp = (NC_Log_metadataentry*)head;
            
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
                stride = NULL;    
            }

            /* Play event */
            
            /* Translate varid to varp */
            err = ncmpii_NC_lookupvar(ncp, entryp->varid, &varp);
            if (err != NC_NOERR){
                return err;
            }
            /* Replay event with non-blocking call */
            err = ncmpii_igetput_varm(ncp, varp, start, count, stride, NULL, (void*)(databuffer + entryp->data_off - databufferidx), -1, buftype, NULL, WRITE_REQ, 0, 0);
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
         * Collective wait
         * Wait must be called first or previous data will be corrupted
         */
        err = ncmpii_wait(ncp, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
        if (err != NC_NOERR) {
            return err;
        }

#ifdef PNETCDF_DEBUG
        tend = MPI_Wtime();
        twait += tend - tbegin;
#endif
        /* Update batch status */
        databufferidx += databatchsize;
        databatchsize = 0;
    }

    /* Free the data buffer */ 
    NCI_Free(databuffer);
    
    /* Flusg complete. Turn off the flushing flag, enable logging on non-blocking call */
    nclogp->isflushing = 0;

#ifdef PNETCDF_DEBUG
    tend = MPI_Wtime();
    ttotal = tend - tstart;

    if (rank == 0){
        printf("Size of data log:       %lld\n", databufferidx + databufferused);
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
            log_flush(ncp);
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
 * Retrieve variable id given NC_var and NC
 * IN    ncp:    NC structure
 * IN    varp:    NC_var structure
 *
 * This function is not used since NC_var contains varid
 */
int get_varid(NC *ncp, NC_var *varp){
    int i;
    NC_vararray ncap = ncp->vars;
    
    /* Search through the variable list for the same address 
     * As varp may not reside in the buffer continuously, a linear search is required
     */
    for(i = 0; i < ncap.ndefined; i++){
        if (ncap.value[i] == varp){
            return i;
        }
    }

    return -1;
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
    case MPI_CHAR:    /* put_*_char */
        itype = NC_LOG_TYPE_SCHAR;
        break;
    case MPI_UNSIGNED_CHAR:    /* put_*_uchar */
        itype = NC_LOG_TYPE_UCHAR;
        break;
    case MPI_BYTE: /* Not corresponding to any api, not used */
        itype = NC_LOG_TYPE_TEXT;
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
    //AppendMetaOffset(nclogp, nclogp->metadata.nused);
    
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
    err = write(nclogp->metalog_fd, buffer, esize);
    if (err < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (err != esize){
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
    err = lseek(nclogp->metalog_fd, sizeof(NC_Log_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->num_entries), SEEK_SET);
    if (err < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of the record
     */
    err = write(nclogp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (err < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (err != SIZEOF_MPI_OFFSET){
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
    if ((err = log_flush(ncp)) != NC_NOERR) {
        return err;
    }
    
    /* Reset log status */
    
    /* Set num_entries to 0 */
    headerp->num_entries = 0;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(nclogp->metalog_fd, sizeof(NC_Log_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->num_entries), SEEK_SET);
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

