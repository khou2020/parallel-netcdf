#ifdef HAVE_CONFIG_H
#include <config.h>
#ifndef WORDS_BIGENDIAN
#define WORDS_BIGENDIAN 0
#endif
#else
/* Define size as computed by sizeof. */
#define SIZEOF_DOUBLE sizeof(double)
#define SIZEOF_FLOAT sozepf(float)
#define SIZEOF_INT sizeof(int)
#define SIZEOF_LONG sizeof(long)
#define SIZEOF_LONG_LONG sizeof(long long)
#define SIZEOF_MPI_OFFSET sizeof(MPI_Offset)
#define SIZEOF_SHORT sizeof(short)
#define SIZEOF_SIGNED_CHAR sizeof(char)
#define SIZEOF_SIZE_T sizeof(size_t)
#define SIZEOF_UINT sizeof(unsigned int)
#define SIZEOF_UNSIGNED_CHAR sizeof(unsigned char)
#define SIZEOF_UNSIGNED_INT sizeof(unsigned int)
#define SIZEOF_UNSIGNED_LONG_LONG sizeof(unsigned long long)
#define SIZEOF_UNSIGNED_SHORT sizeof(unsigned short)
#define WORDS_BIGENDIAN IsBigEndian()
#endif
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
int ncmpii_log_entarray_init(NC_Log_entarray *ep){
    ep->entries = NCI_Malloc(LOG_ARRAY_SIZE * sizeof(NC_Log_metadataentry*));
    if (ep->entries == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    ep->nalloc = LOG_ARRAY_SIZE;
    ep->nused = 0;
    return NC_NOERR;
}

/* 
 * Free the log entry array
 * IN   ep: array to be freed
 */
void ncmpii_log_entarray_free(NC_Log_entarray *ep){
    NCI_Free(ep->entries);
}

/*
 * Append entry to array
 * IN    ep:    array structure
 * IN    ent:    entry to be added
 */
int ncmpii_log_entarray_append(NC_Log_entarray *ep, NC_Log_metadataentry* ent) {
    NC_Log_metadataentry** ret;

    /* Expand array if needed 
     * ep->nused is the size currently in use
     * ep->nalloc is the size of internal buffer
     * If the remaining size is less than the required size, we reallocate the buffer
     */
    if (ep->nalloc < ep->nused + 1) {
        /* 
         * Must make sure realloc successed before increasing ep->nalloc
         * (new size) = (old size) * (META_BUFFER_MULTIPLIER) 
         */
        size_t newsize = ep->nalloc * SIZE_MULTIPLIER;
        /* ret is used to temporaryly hold the allocated buffer so we don't lose nclogp->metadata.buffer if allocation fails */
        ret = (NC_Log_metadataentry**)NCI_Realloc(ep->entries, newsize * sizeof(NC_Log_metadataentry*));
        /* If not enough memory */
        if (ret == NULL) {
            return NC_ENOMEM;
        }
        /* Point to the new buffer and update nalloc */
        ep->entries = ret;
        ep->nalloc = newsize;
    }
    
    /* Add entry to tail */
    ep->entries[ep->nused++] = ent;

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
 * IN    BufferDir:    root directory to store log file
 * IN    Parent:    NC structure that will host the log structure
 * OUT    nclogp:    Initialized log structure 
 */
int ncmpii_log_create(MPI_Comm comm, const char* path, const char* BufferDir, NC *Parent, NC_Log **nclogpp) {
    int ret, rank, np, err, id;
    char logbase[NC_LOG_PATH_MAX];
    char *abspath, *fname;
    size_t ioret;
    NC_Log_metadataheader *headerp;
    NC_Log *nclogp;
    
    /* Get rank and number of processes */
    ret = MPI_Comm_rank(comm, &rank);
    if (ret != MPI_SUCCESS) {
        ret = ncmpii_handle_error(ret, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(ret);
    }
    ret = MPI_Comm_size(comm, &np);
    if (ret != MPI_SUCCESS) {
        ret = ncmpii_handle_error(ret, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(ret);
    }
 
    /* Allocate the structure */
    nclogp = (NC_Log*)NCI_Malloc(sizeof(NC_Log));
    /* Record the parent NC structure */ 
    nclogp->Parent = Parent;

    /* Initialize log structure */
    
    /* Determine log file name
     * Log file name is $(bufferdir)$(basename)_$(ncid)_$(rank).{meta/data}.bin
     * Basename is absolute path to the cdf file
     */

    /* path and BufferDir may contain non-existing directory
     * We need to creat them before realpath can work
     */
    mkpath(path, 0744, 0); 
    mkpath(BufferDir, 0744, 1); 

    /* Resolve absolute path */    
    memset(nclogp->Path, 0, sizeof(nclogp->Path));
    abspath = realpath(path, nclogp->Path);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    abspath = realpath(BufferDir, logbase);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }

    /* Extract file anme 
     * Search for first / charactor from the tail
     * Absolute path should always contains one '/'
     * We return the string including '/' for convenience
     */
    for (fname = nclogp->Path + strlen(nclogp->Path); fname > nclogp->Path; fname--){
        if (*fname == '/'){
            break;
        }
    }
    /* Something wrong if not found */
    if (fname == nclogp->Path){
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);  
    }
    
    /* Log file path may also contain non-existing directory
     * We need to create them before we can search for usable id
     * As log file name hasn't been determined, we need to use a dummy one here
     */
    sprintf(nclogp->metalogpath, "%s%s_%d_%d.meta.bin", logbase, fname, Parent->ncid, rank);
    sprintf(nclogp->datalogpath, "%s%s_%d_%d.data.bin", logbase, fname, Parent->ncid, rank);
    mkpath(nclogp->metalogpath, 0744, 0); 
     
    /* Initialize metadata buffer */
    err = ncmpii_log_buffer_init(&nclogp->metadata);
    if (err != NC_NOERR){
        return err;
    }
       
    /* Initialize metadata entry array */
    err = ncmpii_log_entarray_init(&nclogp->metaentries);
    if (err != NC_NOERR){
        return err;
    }

    /* Set log file descriptor to NULL */
    nclogp->datalog_fd = -1;
    nclogp->metalog_fd = -1;
 
    /* Misc */
    nclogp->Communitator = comm;    /* Communicator passed to ncmpi_create/open */
    nclogp->Flushing = 0;   /* Flushing flag, set to 1 when flushing is in progress, 0 otherwise */
    nclogp->MaxSize = 0;    /* Max size of buffer ever passed to put_var function, not used */ 
    nclogp->UpToDate = 1;   /* Nothing in the log to flush */
    
    /* Initialize metadata header */
    
    /* Allocate space for metadata header */
    headerp = (NC_Log_metadataheader*)ncmpii_log_buffer_alloc(&nclogp->metadata, sizeof(NC_Log_metadataheader));
   
    /* Fill up the metadata log header */
    memset(headerp->magic, 0, sizeof(headerp->magic)); /* Magic */
    strncpy(headerp->magic, NC_LOG_MAGIC, sizeof(headerp->magic));
    memset(headerp->format, 0, sizeof(headerp->format)); /* Format */
    strncpy(headerp->format, NC_LOG_FORMAT_CDF_MAGIC, sizeof(headerp->format));
    memset(headerp->basename, 0, sizeof(headerp->basename)); /* Path */
    strncpy(headerp->basename, nclogp->Path, sizeof(headerp->basename));
    headerp->rank_id = rank;   /* Rank */
    headerp->num_ranks = np;   /* Number of processes */
    headerp->is_external = 0;    /* Without convertion before logging, data in native representation */
    /* Determine endianess */
    if (WORDS_BIGENDIAN) {
        headerp->big_endian = NC_LOG_TRUE;
    }
    else {
        headerp->big_endian = NC_LOG_FALSE;
    }
    headerp->num_entries = 0;    /* The log is empty */
    headerp->max_ndims = 0;    /* Highest dimension among all variables, not used */
    headerp->entry_begin = nclogp->metadata.nused;  /* Location of the first entry */

    /* Create log files */
    
    nclogp->datalog_fd = nclogp->metalog_fd = -1;
    nclogp->metalog_fd = open(nclogp->metalogpath, O_RDWR | O_CREAT | O_TRUNC, 0744);
    if (nclogp->metalog_fd < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
    nclogp->datalog_fd = open(nclogp->datalogpath, O_RDWR | O_CREAT | O_TRUNC, 0744);
    if (nclogp->datalog_fd < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
   
    /* Write metadata header to file
     * Write from the memory buffer to file
     */
    ioret = lseek(nclogp->metalog_fd, 0, SEEK_SET);
    if (ioret < 0){ 
        err = ncmpii_handle_io_error("lseek");
        DEBUG_RETURN_ERROR(err);
    }
    ioret = write(nclogp->metalog_fd, headerp, sizeof(NC_Log_metadataheader));
    if (ioret < 0){
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != sizeof(NC_Log_metadataheader)){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Write data header to file 
     * Data header consists of a fixed sized string PnetCDF0
     */
    ioret = lseek(nclogp->datalog_fd, 0, SEEK_SET);
    if (ioret < 0){ 
        err = ncmpii_handle_io_error("lseek");
        DEBUG_RETURN_ERROR(err);
    }
    ioret = write(nclogp->datalog_fd, "PnetCDF0", 8);
    if (ioret < 0){ 
        err = ncmpii_handle_io_error("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != 8){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* return to the caller */
    *nclogpp = nclogp;

ERROR:;
    if (ret != NC_NOERR) {
        NCI_Free(nclogp);
    }
    
    return ret;
}

/*
 * Create log file for the log structure
 * IN    nclogp:    log structure
 */
int ncmpii_log_enddef(NC_Log *nclogp){   
    int ret = NC_NOERR;

    /* Create log files if not created*/
    if (nclogp->metalog_fd < 0){
        //ret = create_log_file(nclogp);
    }

    return ret;
}

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN    nclogp:    log structure
 */
int log_flush(NC_Log *nclogp) {
    int i, ret, fd;
    size_t dsize, dbsize;
    ssize_t ioret;
    struct stat metastat;
    NC_Log_metadataentry *entryp;
    NC_var *varp;
    MPI_Offset dblow, dbup;
    MPI_Offset *start, *count, *stride;
    MPI_Datatype buftype;
    char *data, *head, *tail;
    NC_Log_metadataheader* headerp;

#ifdef PNETCDF_DEBUG
    int rank;
    double tstart, tend, tread = 0, twait = 0, treplay = 0, tbegin, ttotal;
    
    MPI_Comm_rank(nclogp->Communitator, &rank);

    tstart = MPI_Wtime();
#endif

    /* Turn on the flushing flag so non-blocking call won't be logged */
    nclogp->Flushing = 1;

    /* Read datalog in to memory */
   
    /* Get data log size 
     * Since we don't delete log file after flush, the file size is not the size of data log
     * Use lseek to find the current position of data log descriptor
     * NOTE: For now, we assume descriptor always points to the end of the last record
     */
    ioret = lseek(nclogp->datalog_fd, 0, SEEK_CUR);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    dsize = ioret;
    
    /* 
     * Prepare data buffer
     * We determine the data buffer size according to:
     * hints, size of data log, the largest size of single record
     * 0 in hint means no limit
     * (Buffer size) = max((largest size of single record), min((size of data log), (size specified in hint)))
     */
    dbsize = dsize;
    if (dbsize > nclogp->FlushBufferSize){
        dbsize = nclogp->FlushBufferSize;
    }
    if (dbsize < nclogp->MaxSize){
        dbsize = nclogp->MaxSize;
    }
    /* Allocate buffer */
    data = (char*)NCI_Malloc(dbsize);
    /* Set lower and upper bound to 0 */
    dblow = dbup = 0;

    /* Iterate through meta log entries */
    headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
    head = nclogp->metadata.buffer + headerp->entry_begin;   /* Head points to the current metadata entry */
    tail = head + sizeof(NC_Log_metadataentry); /* Tail points to the end of current metadata entry header, the locaation of start, count, and stride */
    for (i = 0; i < headerp->num_entries; i++) {
        /* Treate the data at head as a metadata entry header */
        entryp = (NC_Log_metadataentry*)head;

        /* 
         * Read from data log if data not in buffer 
         * We don't need to check lower bound when data is logged monotonically
         * We also wait for replayed entries to free memory
         */
        if (entryp->data_off + entryp->data_len >= dbup){
            size_t rsize;
#ifdef PNETCDF_DEBUG
            tbegin = MPI_Wtime();
#endif
            /* 
             * Collective wait
             * Wait must be called first or previous data will be corrupted
             */
            ret = ncmpii_wait(nclogp->Parent, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
            if (ret != NC_NOERR) {
                return ret;
            }

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            twait += tend - tbegin;
            tbegin = MPI_Wtime();
#endif
            /* Update buffer lower bound */
            dblow = entryp->data_off;
            /* Seek to new lower bound */
            ioret = lseek(nclogp->datalog_fd, dblow, SEEK_SET);
            if (ioret < 0){
                DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
            }
            /* 
             * Read data to buffer
             * We calculate the size to read based on data buffer size, and remaining of the data log
             * In this case, we know there is an error if actual size read is not expected
             */
            if (dsize - dblow < dbsize){
                rsize = dsize - dblow;   
            }
            else{
                rsize = dbsize;
            }
            ioret = read(nclogp->datalog_fd, data, rsize); 
            if (ioret < 0) {
                ioret = ncmpii_handle_io_error("read");
                if (ioret == NC_EFILE){
                    ioret = NC_EREAD;
                }
                DEBUG_RETURN_ERROR(ioret);
            }
            if (ioret != rsize){
                DEBUG_RETURN_ERROR(NC_EBADLOG);
            }
            /* Update buffer upper bound */
            dbup = dblow + rsize;

#ifdef PNETCDF_DEBUG
            tend = MPI_Wtime();
            tread += tend - tbegin;
#endif
        }

#ifdef PNETCDF_DEBUG
        tbegin = MPI_Wtime();
#endif
        /* start, count, stride */
        start = (MPI_Offset*)tail;
        count = start + entryp->ndims;
        stride = count + entryp->ndims;

        /* Convert from log type to MPI type used by pnetcdf library
         * Log spec has different enum of types than MPI
         */
        switch (entryp->itype) {
        case NC_LOG_TYPE_NATIVE:
            buftype = MPI_CHAR;
            break;
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
            buftype = MPI_CHAR;
        }
        
        /* Determine API_Kind */
        if (entryp->api_kind == NC_LOG_API_KIND_VARA){
            stride = NULL;    
        }

        /* Play event */
        
        /* Translate varid to varp */
        ret = ncmpii_NC_lookupvar(nclogp->Parent, entryp->varid, &varp);
        if (ret != NC_NOERR){
            return ret;
        }
        /* Replay event with non-blocking call */
        ret = ncmpii_igetput_varm(nclogp->Parent, varp, start, count, stride, NULL, (void*)(data + entryp->data_off - dblow), -1, buftype, NULL, WRITE_REQ, 0, 0);
        if (ret != NC_NOERR) {
            return ret;
        }

        /* Move to next record */
        head += entryp->esize;
        tail = head + sizeof(NC_Log_metadataentry);

#ifdef PNETCDF_DEBUG
        tend = MPI_Wtime();
        treplay += tend - tbegin;
#endif
    }

#ifdef PNETCDF_DEBUG
    tbegin = MPI_Wtime();
#endif

    /* Collective wait */
    ret = ncmpii_wait(nclogp->Parent, NC_PUT_REQ_ALL, NULL, NULL, COLL_IO);
    if (ret != NC_NOERR) {
        return ret;
    }

#ifdef PNETCDF_DEBUG
    tend = MPI_Wtime();
    twait += tend - tbegin;
#endif

    /* Free the data buffer */ 
    NCI_Free(data);
    
    /* Flusg complete. Turn off the flushing flag, enable logging on non-blocking call */
    nclogp->Flushing = 0;

#ifdef PNETCDF_DEBUG
    tend = MPI_Wtime();
    ttotal = tend - tstart;

    if (rank == 0){
        printf("Size of data log:       %lld\n", dsize);
        printf("Size of data buffer:    %lld\n", dbsize);
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
int ncmpii_log_close(NC_Log *nclogp) {
    int ret;
    
    /* If log file is created, flush the log */
    if (nclogp->metalog_fd >= 0){
        /* Commit to CDF file */
        if (!nclogp->UpToDate){
            log_flush(nclogp);
        }

        /* Close log file */
        ret = close(nclogp->metalog_fd);
        ret |= close(nclogp->datalog_fd);
        if (ret < 0){
            DEBUG_RETURN_ERROR(ncmpii_handle_io_error("close"));        
        }

        /* Delete log files if delete flag is set */
        if (nclogp->DeleteOnClose){
            remove(nclogp->datalogpath);
            remove(nclogp->metalogpath);
        }
    }

    /* Free meta data buffer and metadata offset list*/
    ncmpii_log_buffer_free(&nclogp->metadata);
    ncmpii_log_entarray_free(&nclogp->metaentries);

    /* Delete log structure */
    NCI_Free(nclogp);

    return NC_NOERR;
}


#if (X_SIZEOF_INT != SIZEOF_INT)
/*
 * Write metadata to metadata log in external format
 * This function is not used
 *
 * IN    nclogp: log structure
 * IN    E: metadata entry header
 * IN    start: start in put_var* call
 * IN    count: count in put_var* call
 * IN    stride: stride in put_var* call
 */
int WriteXMeta(NC_Log *nclogp, NC_Log_metadataentry *entryp, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[]) {
    size_t xsize, size;
    char *buffer;
    
    /* External size */
    xsize = NC_LOG_MAGIC_SIZE + NC_LOG_FORMAT_SIZE + SIZEOF_MPI_OFFSET * (5 + 3 * entryp->ndims) + X_SIZEOF_INT * 2;

    /* Seek needs old Metahead 
     * Partial record may exist, so can not use EOF as start point
     * External size is different than internal
     */
    lseek(nclogp->metalog_fd, nclogp->num_entrie * xsize, SEEK_SET);   

    /* Write to disk and update the log structure */
    write(nclogp->metalog_fd, buffer, size);
    nclogp->MetaHeader.num_entries++;
    lseek(nclogp->metalog_fd, sizeof(nclogp->MetaHeader) - sizeof(nclogp->MetaHeader.basename) - sizeof(nclogp->MetaHeader.num_entries), SEEK_SET);    /* Note: location need to be updated when struct change */
    write(nclogp->metalog_fd, &nclogp->MetaHeader.num_entries, SIZEOF_MPI_OFFSET);    /* This marks the completion of the record */

    return NC_NOERR;
}
#endif

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
int ncmpii_log_put_var(NC_Log *nclogp, NC_var *varp, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Datatype buftype, int PackedSize){
    int ret, vid, dim;
    int itype;    /* Type used in log file */
    char *buffer;
    MPI_Offset esize, dataoff;
    MPI_Offset *Start, *Count, *Stride;
    ssize_t ioret;
    NC_Log_metadataentry *entryp;
    NC_Log_metadataheader *headerp;

    /* Enddef must be called at least once */
    if (nclogp->metalog_fd < 0){
        DEBUG_RETURN_ERROR(NC_ELOGNOTINIT);        
    }

    /* Get variable id and dimension */
    dim = varp->ndims;
    vid = varp->varid;

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
    case MPI_SHORT:    /* put_*_short */
        itype = NC_LOG_TYPE_SHORT;
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
        itype = NC_LOG_TYPE_NATIVE; 
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
    ioret = lseek(nclogp->datalog_fd, 0, SEEK_CUR);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    dataoff = (MPI_Offset)ioret;
    
    /* Write data log */
    ioret = write(nclogp->datalog_fd, buf, PackedSize);
    if (ioret < 0){
        ret = ncmpii_handle_io_error("write");
        if (ret == NC_EFILE){
            ret = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(ret);
    }
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
    ret = lseek(nclogp->metalog_fd, nclogp->metadata.nused, SEEK_SET);   
    if (ret < 0){
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
    entryp->varid = vid;  /* Variable id */
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

    /* Record the maximum size of data
     * In a batched or 1 by 1 flushing approach, we need to know the size of data buffer needed
     * This is not used for current implementation
     */
    if (entryp->data_len > nclogp->MaxSize) {
        nclogp->MaxSize = entryp->data_len;
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
    ret = write(nclogp->metalog_fd, buffer, esize);
    if (ret < 0){
        ret = ncmpii_handle_io_error("write");
        if (ret == NC_EFILE){
            ret = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(ret);
    }
    if (ret != esize){
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
    ret = lseek(nclogp->metalog_fd, sizeof(NC_Log_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->num_entries), SEEK_SET);
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of the record
     */
    ret = write(nclogp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ret < 0){
        ret = ncmpii_handle_io_error("write");
        if (ret == NC_EFILE){
            ret = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(ret);
    }
    if (ret != SIZEOF_MPI_OFFSET){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Mark netcdf file as outdated */
    nclogp->UpToDate = 0;

    return NC_NOERR;
}

/*
 * Commit the log into cdf file and delete the log
 * User can call this to force a commit without closing
 * It work by flush and re-initialize the log structure
 * IN    nclogp:    log structure
 */
int ncmpii_log_flush(NC_Log *nclogp) {
    int ret;
    size_t ioret;
    NC_Log_metadataheader *headerp;

    /* Nothing to replay if nothing have been written */
    if (nclogp->UpToDate){
        return NC_NOERR;
    }
    
    /* Replay log file */
    if ((ret = log_flush(nclogp)) != NC_NOERR) {
        return ret;
    }
    
    /* Reset log status */
    
    /* Everything is flushed */
    nclogp->UpToDate = 1;

    /* Set num_entries to 0 */
    headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
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
        ret = ncmpii_handle_io_error("write");
        if (ret == NC_EFILE){
            ret = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(ret);
    }
    if (ioret != SIZEOF_MPI_OFFSET){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Reset buffer status */
    nclogp->metadata.nused = headerp->entry_begin;
    nclogp->metaentries.nused = 0;
        
    /* Rewind data log file descriptors */
    ioret = lseek(nclogp->datalog_fd, 8, SEEK_SET);
    if (ioret != 8){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }

    return NC_NOERR;
}

