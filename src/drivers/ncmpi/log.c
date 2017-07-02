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

#define META_BUFFER_SIZE 1024 /* Size of initial metadata buffer */
#define META_OFFSET_BUFFER_SIZE 32 /* Size of initial metadata offset list */    
#define META_BUFFER_MULTIPLIER 20    /* When metadata buffer is full, we'll NCI_Reallocate it to META_BUFFER_MULTIPLIER times the original size*/

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

/* Write int array as 32 bit int to fle
 *
 * IN    fd:    file descriptor for unix io
 * IN    buf:    data to be written 
 * IN    count:    number of element to write    
 */
/*
int WriteInt32(int fd, int *buf, int count){
    ssize_t ret;
#if X_SIZEOF_INT == SIZEOF_INT
    ret = write(fd, buf, count * SIZEOF_INT);
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("write"));
    }
    if (ret != count * SIZEOF_INT){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
#else
    int i, j;
    char* tmp;
    tmp = (char*)NCI_Malloc(X_SIZEOF_INT * count);
    if (tmp == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    for(i = 0; i < count; i++){
        for (j = 0; j < X_SIZEOF_INT; j++){
#ifdef WORDS_BIGENDIAN
            tmp[i * X_SIZEOF_INT + j] = (buf[i] >> ((X_SIZEOF_INT - j + 1) * 8)) & 0xff;
#else
            tmp[i * X_SIZEOF_INT + j] = (buf[i] >> (j * 8)) & 0xff;
#endif
        }
    }
    ret = write(fd, tmp, count * X_SIZEOF_INT);
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("write"));
    }
    if (ret != count * SIZEOF_INT){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
 
    NCI_Free(tmp);
#endif
    return NC_NOERR;
}
*/

/* Read 32 bit int from file to int array
 *
 * IN   fd:    file descriptor for unix io
 * OUT  buf:    data to be written 
 * IN   count:    number of element to write    
 */
/*
int ReadInt32(int fd, int *buf, int count){
    ssize_t ret;
#if X_SIZEOF_INT == SIZEOF_INT
    ret = read(fd, buf, count * SIZEOF_INT);
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("reed"));
    }
    if (ret != count * SIZEOF_INT){
        DEBUG_RETURN_ERROR(NC_EREAD);
    }
#else
    int i, j;
    char* tmp;
    tmp = (char*)NCI_Malloc(X_SIZEOF_INT * count);
    if (tmp == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    ret = read(fd, tmp, count * X_SIZEOF_INT);
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("reed"));
    }
    if (ret != count * SIZEOF_INT){
        DEBUG_RETURN_ERROR(NC_EREAD);
    }

    for(i = 0; i < count; i++){
        buf[i] = 0;
        for (j = 0; j < X_SIZEOF_INT; j++){
#ifdef WORDS_BIGENDIAN
            buf[i] |= ((int)tmp[i * X_SIZEOF_INT + j] << ((X_SIZEOF_INT - j + 1) * 8));
#else
            buf[i] |= ((int)tmp[i * X_SIZEOF_INT + j] << (j * 8));
#endif
        }
    }
    NCI_Free(tmp);
#endif
    return NC_NOERR;
}
*/

/*
 * Record metadata offset into the metadata offset list for fast retrival
 * This is similar to meta_alloc, but we allocate SIZEOF_SIZE_T for one offset and put it in instead of returns the buffer
 * IN    nclogp:    log structure
 * IN    offset:    offset of the new metadata entry
 */
int AppendMetaOffset(NC_Log *nclogp, size_t offset) {
    size_t* ret;

    /* Expand buffer if needed
     * If MetaOffset is full, we reallocate it to a larger size
     */
    if (nclogp->MetaOffsetBufferSize < nclogp->MetaOffsetSize + 1) {
        /* (new size) = (old size) * (META_BUFFER_MULTIPLIER) */
        nclogp->MetaOffsetBufferSize *= META_BUFFER_MULTIPLIER;
        /* ret is used to temporaryly hold the allocated buffer so we don't lose nclogp->Metadata if allocation fails */
        ret = (size_t*)NCI_Realloc(nclogp->MetaOffset, nclogp->MetaOffsetBufferSize * SIZEOF_SIZE_T);
        /* If not enough memory */
        if (ret == NULL) {
            DEBUG_RETURN_ERROR(NC_ENOMEM);
        }
        /* Point to the new buffer */
        nclogp->MetaOffset = ret;
    }

    /* Record the offsest and increase the tail pointer (MetaOffsetSize) */
    nclogp->MetaOffset[nclogp->MetaOffsetSize++] = offset;
    
    return NC_NOERR;
}

/*
 * Allocate space in the metadata buffer
 * This function works as NCI_Malloc in the metadata buffer
 * IN    nclogp:    log structure
 * IN    size:    size required in the buffer
 */
char* meta_alloc(NC_Log *nclogp, size_t size) {
    char* ret;

    /* Expand buffer if needed 
     * nclogp->MetaSize is the size currently in use
     * nclogp->MetaBufferSize is the size of metadata buffer
     * If the remaining size is less than the required size, we reallocate the buffer
     */
    if (nclogp->MetaBufferSize < nclogp->MetaSize + size) {
        /* We don't know how large the required size is, loop until we have enough space */
        while (nclogp->MetaBufferSize < nclogp->MetaSize + size) {
            /* (new size) = (old size) * (META_BUFFER_MULTIPLIER) */
            nclogp->MetaBufferSize *= META_BUFFER_MULTIPLIER;
        }
        /* ret is used to temporaryly hold the allocated buffer so we don't lose nclogp->Metadata if allocation fails */
        ret = (char*)NCI_Realloc(nclogp->Metadata, nclogp->MetaBufferSize);
        /* If not enough memory */
        if (ret == NULL) {
            return ret;
        }
        /* Point to the new buffer */
        nclogp->Metadata = ret;
    }
    
    /* Increase used buffer size and return the allocated space */
    ret = nclogp->Metadata + nclogp->MetaSize;
    nclogp->MetaSize += size;
    return ret;
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
    NC_Log_metadataheader *H;
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
     * Log file name is $(bufferdir)$(basename)_$(rank).{meta/data}.bin
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
    sprintf(nclogp->MetaPath, "%s%s.meta.bin", logbase, fname);
    mkpath(nclogp->MetaPath, 0744, 0); 

    /* Searching for avaiable logid that won't cause a file conflict
     * If there's no conflict on process 0, we assume no conflict on all processes
     * If there's no conflict on metadata log, we assume no conflict on data log
     */
    if (rank == 0){
        for(id = 0;;id++){
            sprintf(nclogp->MetaPath, "%s%s_%d_%d.meta.bin", logbase, fname, id, rank);
#ifdef HAVE_ACCESS
            /* Break if file not exists */
            if (access(nclogp->MetaPath, F_OK) < 0){
                break;
            }
#else
            /* Try opening */
            ret = open(nclogp->MetaPath, O_RDONLY);
            /* Break if file not exists */
            if (ret < 0 && errno == ENOENT){
                break;
            }
#endif
        }
    }
    ret = MPI_Bcast(&id, 1, MPI_INT, 0, comm);
    if (ret != MPI_SUCCESS) {
        err = ncmpii_handle_error(ret, "MPI_Bcast log id");
    }
    sprintf(nclogp->MetaPath, "%s%s_%d_%d.meta.bin", logbase, fname, id, rank);
    sprintf(nclogp->DataPath, "%s%s_%d_%d.data.bin", logbase, fname, id, rank);
    
    /* Initialize metadata buffer */
    nclogp->MetaBufferSize = META_BUFFER_SIZE;  /* Size of metadata buffer */
    nclogp->MetaSize = 0;   /* Size of metadata buffer in use */
    nclogp->Metadata = (char*)NCI_Malloc(nclogp->MetaBufferSize); /* Allocate metadata buffer */
    if (nclogp->Metadata == NULL) {
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    
    /* Initialize metadata offset list */
    nclogp->MetaOffsetBufferSize = META_OFFSET_BUFFER_SIZE; /* Length of metadata offset list */
    nclogp->MetaOffsetSize = 0; /* Size of metadata offset list in use = number of metadata entry */
    nclogp->MetaOffset = (size_t*)NCI_Malloc(nclogp->MetaOffsetBufferSize * SIZEOF_SIZE_T);
    if (nclogp->MetaOffset == NULL) {
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }

    /* Set log file descriptor to NULL */
    nclogp->DataLog = -1;
    nclogp->MetaLog = -1;
 
    /* Misc */
    nclogp->Communitator = comm;    /* Communicator passed to ncmpi_create/open */
    nclogp->Flushing = 0;   /* Flushing flag, set to 1 when flushing is in progress, 0 otherwise */
    nclogp->MaxSize = 0;    /* Max size of buffer ever passed to put_var function, not used */ 
    nclogp->LogId = id;    /* Id associate with the log structure to prevent file name conflict */ 
    nclogp->UpToDate = 1;   /* Nothing in the log to flush */
    
    /* Initialize metadata header */
    
    /* Allocate space for metadata header */
    H = (NC_Log_metadataheader*)meta_alloc(nclogp, sizeof(NC_Log_metadataheader));
   
    /* Fill up the metadata log header */
    memset(H->magic, 0, sizeof(H->magic)); /* Magic */
    strncpy(H->magic, NC_LOG_MAGIC, sizeof(H->magic));
    memset(H->format, 0, sizeof(H->format)); /* Format */
    strncpy(H->format, NC_LOG_FORMAT_CDF_MAGIC, sizeof(H->format));
    memset(H->basename, 0, sizeof(H->basename)); /* Path */
    strncpy(H->basename, nclogp->Path, sizeof(H->basename));
    H->rank_id = rank;   /* Rank */
    H->num_ranks = np;   /* Number of processes */
    H->is_external = 0;    /* Without convertion before logging, data in native representation */
    /* Determine endianess */
    if (WORDS_BIGENDIAN) {
        H->big_endian = NC_LOG_TRUE;
    }
    else {
        H->big_endian = NC_LOG_FALSE;
    }
    H->num_entries = 0;    /* The log is empty */
    H->max_ndims = 0;    /* Highest dimension among all variables, not used */
    H->entry_begin = nclogp->MetaSize;  /* Location of the first entry */

    
    /* Create log file */
    
    
    /* Create log files */
    
    nclogp->DataLog = nclogp->MetaLog = -1;
    nclogp->MetaLog = open(nclogp->MetaPath, O_RDWR | O_CREAT | O_TRUNC, 0744);
    if (nclogp->MetaLog < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
    nclogp->DataLog = open(nclogp->DataPath, O_RDWR | O_CREAT | O_TRUNC, 0744);
    if (nclogp->DataLog < 0) {
        err = ncmpii_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
   
    /* Write metadata header to file
     * Write from the memory buffer to file
     */
    ioret = lseek(nclogp->MetaLog, 0, SEEK_SET);
    if (ioret < 0){ 
        err = ncmpii_handle_io_error("lseek");
        DEBUG_RETURN_ERROR(err);
    }
    ioret = write(nclogp->MetaLog, H, sizeof(NC_Log_metadataheader));
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
    ioret = lseek(nclogp->DataLog, 0, SEEK_SET);
    if (ioret < 0){ 
        err = ncmpii_handle_io_error("lseek");
        DEBUG_RETURN_ERROR(err);
    }
    ioret = write(nclogp->DataLog, "PnetCDF0", 8);
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
    if (nclogp->MetaLog < 0){
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
    NC_Log_metadataentry *E;
    NC_var *varp;
    MPI_Offset dblow, dbup;
    MPI_Offset *start, *count, *stride;
    MPI_Datatype buftype;
    char *data, *head, *tail;
    NC_Log_metadataheader* H;

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
    ioret = lseek(nclogp->DataLog, 0, SEEK_CUR);
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
    if (dbsize > nclogp->DataBufferSize){
        dbsize = nclogp->DataBufferSize;
    }
    if (dbsize < MaxSize){
        dbsize = MaxSize;
    }
    /* Allocate buffer */
    data = (char*)NCI_Malloc(dbsize);
    /* Set lower and upper bound to 0 */
    dblow = dbup = 0;

    /* Iterate through meta log entries */
    H = (NC_Log_metadataheader*)nclogp->Metadata;
    head = nclogp->Metadata + H->entry_begin;   /* Head points to the current metadata entry */
    tail = head + sizeof(NC_Log_metadataentry); /* Tail points to the end of current metadata entry header, the locaation of start, count, and stride */
    for (i = 0; i < H->num_entries; i++) {
        /* Treate the data at head as a metadata entry header */
        E = (NC_Log_metadataentry*)head;

        /* 
         * Read from data log if data not in buffer 
         * We don't need to check lower bound when data is logged monotonically
         * We also wait for replayed entries to free memory
         */
        if (E->data_off + E->data_len >= dbup){
            size_t rsize;
#ifdef PNETCDF_DEBUG
            tbegin = MPI_Wtime();
#endif
            /* Update buffer lower bound */
            dblow = E->data_off;
            /* Seek to new lower bound */
            ioret = lseek(nclogp->DataLog, dblow, SEEK_SET);
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
            ioret = read(nclogp->DataLog, data, rsize); 
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
        }

#ifdef PNETCDF_DEBUG
        tbegin = MPI_Wtime();
#endif
        /* start, count, stride */
        start = (MPI_Offset*)tail;
        count = start + E->ndims;
        stride = count + E->ndims;

        /* Convert from log type to MPI type used by pnetcdf library
         * Log spec has different enum of types than MPI
         */
        switch (E->itype) {
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
        if (E->api_kind == NC_LOG_API_KIND_VARA){
            stride = NULL;    
        }

        /* Play event */
        
        /* Translate varid to varp */
        ret = ncmpii_NC_lookupvar(nclogp->Parent, E->varid, &varp);
        if (ret != NC_NOERR){
            return ret;
        }
        /* Replay event with non-blocking call */
        ret = ncmpii_igetput_varm(nclogp->Parent, varp, start, count, stride, NULL, (void*)(data + E->data_off - dblow), -1, buftype, NULL, WRITE_REQ, 0, 0);
        if (ret != NC_NOERR) {
            return ret;
        }

        /* Move to next record */
        head += E->esize;
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
        printf("Size of metadata log:   %lld\n", nclogp->MetaSize);
        printf("Number of log entries:  %lld\n", H->num_entries);
    
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
    if (nclogp->MetaLog >= 0){
        /* Commit to CDF file */
        if (!nclogp->UpToDate){
            flush_log(nclogp);
        }

        /* Close log file */
        ret = close(nclogp->MetaLog);
        ret |= close(nclogp->DataLog);
        if (ret < 0){
            DEBUG_RETURN_ERROR(ncmpii_handle_io_error("close"));        
        }

        /* Delete log files if delete flag is set */
        if (nclogp->DeleteOnClose){
            remove(nclogp->DataPath);
            remove(nclogp->MetaPath);
        }
    }

    /* Free meta data buffer and metadata offset list*/
    NCI_Free(nclogp->Metadata);
    NCI_Free(nclogp->MetaOffset);

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
int WriteXMeta(NC_Log *nclogp, NC_Log_metadataentry *E, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[]) {
    size_t xsize, size;
    char *buffer;
    
    /* External size */
    xsize = NC_LOG_MAGIC_SIZE + NC_LOG_FORMAT_SIZE + SIZEOF_MPI_OFFSET * (5 + 3 * E->ndims) + X_SIZEOF_INT * 2;

    /* Seek needs old Metahead 
     * Partial record may exist, so can not use EOF as start point
     * External size is different than internal
     */
    lseek(nclogp->MetaLog, nclogp->num_entrie * xsize, SEEK_SET);   

    /* Write to disk and update the log structure */
    write(nclogp->MetaLog, buffer, size);
    nclogp->MetaHeader.num_entries++;
    lseek(nclogp->MetaLog, sizeof(nclogp->MetaHeader) - sizeof(nclogp->MetaHeader.basename) - sizeof(nclogp->MetaHeader.num_entries), SEEK_SET);    /* Note: location need to be updated when struct change */
    write(nclogp->MetaLog, &nclogp->MetaHeader.num_entries, SIZEOF_MPI_OFFSET);    /* This marks the completion of the record */

    return NC_NOERR;
}
#endif

/*
 * Retrieve variable id given NC_var and NC
 * IN    ncp:    NC structure
 * IN    varp:    NC_var structure
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
    NC_Log_metadataentry *E;
    NC_Log_metadataheader *H;

    /* Enddef must be called at least once */
    if (nclogp->MetaLog < 0){
        DEBUG_RETURN_ERROR(NC_ELOGNOTINIT);        
    }

    /* Get variable id and dimension */
    dim = varp->ndims;
    vid = get_varid(nclogp->Parent, varp);
            
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
    ioret = lseek(nclogp->DataLog, 0, SEEK_CUR);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    dataoff = (MPI_Offset)ioret;
    
    /* Write data log */
    ioret = write(nclogp->DataLog, buf, PackedSize);
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
    AppendMetaOffset(nclogp, nclogp->MetaSize);
    
    /* Seek to the head of metadata
     * Note: EOF may not be the place for next entry after a flush
     * Note: metadata size will be updated after allocating metadata buffer space, seek must be done first 
     */
    ret = lseek(nclogp->MetaLog, nclogp->MetaSize, SEEK_SET);   
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }

    /* Size of metadata entry
     * Include metadata entry header and variable size additional data (start, count, stride) 
     */
    esize = sizeof(NC_Log_metadataentry) + dim * 3 * SIZEOF_MPI_OFFSET;
    
    /* Allocate space for metadata entry header */
    buffer = meta_alloc(nclogp, esize);
    E = (NC_Log_metadataentry*)buffer;
    E->esize = esize; /* Entry size */
    E->itype = itype; /* Variable type */
    E->varid = vid;  /* Variable id */
    E->ndims = dim;  /* Number of dimensions of the variable*/
	E->data_len = PackedSize; /* The size of data in bytes. The size that will be write to data log */

    /* Find out the location of data in datalog
     * Which is current possition in data log 
     * Datalog descriptor should always points to the end of file
     */
    ioret = lseek(nclogp->DataLog, 0, SEEK_CUR);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    E->data_off = dataoff;
    
    
    /* Determine the api kind of original call
     * If stride is NULL, we log it as a vara call, otherwise, a vars call 
     * Upper layer translates var1 and var to vara, so we only have vara and vars
     */
    if (stride == NULL){
        E->api_kind = NC_LOG_API_KIND_VARA;
    }
    else{
        E->api_kind = NC_LOG_API_KIND_VARS;
    }

    /* Record the maximum size of data
     * In a batched or 1 by 1 flushing approach, we need to know the size of data buffer needed
     * This is not used for current implementation
     */
    if (E->data_len > nclogp->MaxSize) {
        nclogp->MaxSize = E->data_len;
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
    ret = write(nclogp->MetaLog, buffer, esize);
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
    H = (NC_Log_metadataheader*)nclogp->Metadata;
    H->num_entries++;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ret = lseek(nclogp->MetaLog, sizeof(NC_Log_metadataheader) - sizeof(H->basename) - sizeof(H->num_entries), SEEK_SET);
    if (ret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of the record
     */
    ret = write(nclogp->MetaLog, &H->num_entries, SIZEOF_MPI_OFFSET);
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
    NC_Log_metadataheader *H;

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
    H = (NC_Log_metadataheader*)nclogp->Metadata;
    H->num_entries = 0;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(nclogp->MetaLog, sizeof(NC_Log_metadataheader) - sizeof(H->basename) - sizeof(H->num_entries), SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of flush
     */
    ioret = write(nclogp->MetaLog, &H->num_entries, SIZEOF_MPI_OFFSET);
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
    nclogp->MetaSize = H->entry_begin;
    nclogp->MetaOffsetSize = 0;
        
    /* Rewind data log file descriptors */
    ioret = lseek(nclogp->DataLog, 8, SEEK_SET);
    if (ioret != 8){
        DEBUG_RETURN_ERROR(ncmpii_handle_io_error("lseek"));
    }

    return NC_NOERR;
}

