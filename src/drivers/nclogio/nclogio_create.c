/*
 *  Copyright (C) 2003, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */
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
 * Create a netcdf file with log io
 * IN    comm:    communicator passed to ncmpi_open
 * IN    path:    path of the CDF file
 * IN    cmode:   cmode of the CDF file
 * IN    ncid:    ncid of the CDF file
 * IN    info:    path of the CDF file
 * OUT   ncdp:    Initialized log structure 
 */
int nclogio_create(MPI_Comm comm, const char *path, int cmode, int ncid, 
                        MPI_Info info, void **ncdp) {
    int err, status = NC_NOERR;
    NC_Log *nclogp;

    /* Allocate the structure */
    nclogp = (NC_Log*)malloc(sizeof(NC_Log));
    /* Get ncmpio dispatcher */
    nclogp->ncmpio_dispatcher = ncmpio_inq_dispatcher();
   
    /* Create netcdf file with ncmpio driver */
    err = nclogp->ncmpio_dispatcher->create(comm, path, cmode, 
                    ncid, info, &nclogp->ncp);
    if (status == NC_NOERR){
        status = err;
    }
    /* Initialize the log */
    err = log_init(nclogp, comm, path, info);
    if (status == NC_NOERR){
        status = err;
    }
        
    /* Return to caller */
    *ncdp = nclogp;

ERROR:;
    if (status != NC_NOERR){
        free(nclogp);
    }

    return status;
}

/*
 * open a cdffile with log io
 * IN    comm:    communicator passed to ncmpi_open
 * IN    path:    path of the CDF file
 * IN    cmode:   cmode of the CDF file
 * IN    ncid:    ncid of the CDF file
 * IN    info:    path of the CDF file
 * OUT   ncdp:    Initialized log structure 
 */
int nclogio_open(MPI_Comm comm, const char *path, int omode, int ncid, 
                    MPI_Info info, void **ncdp) {
    int err, status = NC_NOERR;
    NC_Log *nclogp;
    
    /* Allocate the structure */
    nclogp = (NC_Log*)malloc(sizeof(NC_Log));
    /* Get ncmpio dispatcher */
    nclogp->ncmpio_dispatcher = ncmpio_inq_dispatcher();
   
    /* Create netcdf file with ncmpio driver */
    err = nclogp->ncmpio_dispatcher->open(comm, path, omode, ncid, 
                    info, &nclogp->ncp);
    if (status == NC_NOERR){
        status = err;
    }

    /* Initialize the log */
    err = log_init(nclogp, comm, path, info);
    if (status == NC_NOERR){
        status = err;
    }
    
    /* Create log file */
    err = log_create(nclogp);
    if (status == NC_NOERR){
        status = err;
    }
 
    /* Return to caller */
    *ncdp = nclogp;
    
ERROR:;
    if (status != NC_NOERR){
        free(nclogp);
    }

    return status;
}

/*
 * Initialize a new log structure
 * IN    comm:    communicator passed to ncmpi_open
 * IN    path:    path of the CDF file
 * IN    cmode:   cmode of the CDF file
 * IN    ncid:    ncid of the CDF file
 * IN    info:    path of the CDF file
 * OUT   ncdp:    Initialized log structure 
 */
int log_init(NC_Log *nclogp, MPI_Comm comm, const char *path, 
                MPI_Info info) {
    int i, err, flag, hintflag;
    char logbase[NC_LOG_PATH_MAX], basename[NC_LOG_PATH_MAX];
    char hint[MPI_MAX_INFO_VAL];
    char *abspath, *fname;
    DIR *logdir;
    size_t ioret, headersize;
    NC_Log_metadataheader *headerp;
    NC* ncp = (NC*)nclogp->ncp;

    /* Get rank and number of processes */
    err = MPI_Comm_rank(comm, &nclogp->rank);
    if (err != MPI_SUCCESS) {
        err = nclogio_handle_error(err, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(err);
    }
    err = MPI_Comm_size(comm, &nclogp->np);
    if (err != MPI_SUCCESS) {
        err = nclogio_handle_error(err, "MPI_Comm_rank");
        DEBUG_RETURN_ERROR(err);
    }
    
    /* Initialize log structure */
    
    /* Determine log file name
     * Log file name is $(bufferdir)$(basename)_$(ncid)_$(rank).{meta/data}
     * filepath is absolute path to the cdf file
     */

    /* Get log base dir from hint */
    if (info != MPI_INFO_NULL) {
        MPI_Info_get(info, "pnetcdf_log_base", MPI_MAX_INFO_VAL - 1, hint, &hintflag);
    }
    else{
        hintflag = 0;
    }
    if (!hintflag) {
        strncpy(hint, ".", 2);
    }
    
    /* 
     * Make sure bufferdir exists 
     * NOTE: Assume upper layer already check for directory along netcdf file path
     */
    logdir = opendir(hint);
    if (logdir == NULL) {
        /* Log base does not exist or not accessible */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    closedir(logdir);
    /* Path to log dir */
    abspath = realpath(hint, logbase);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    
    /* 
     * Path of netcdf file
     * We assume the file exists after ncmpio_create returns no error
     */
    memset(nclogp->filepath, 0, sizeof(nclogp->filepath));
    abspath = realpath(path, nclogp->filepath);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
   
    /* Extract file anme 
     * Search for first / charactor from the tail
     * Absolute path should always contains one '/', return error otherwise
     * We return the string including '/' for convenience
     */
    fname = strrchr(nclogp->filepath, '/');
    if (fname == NULL){
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);  
    }
    
    /* Log file path may also contain non-existing directory
     * We need to create them before we can search for usable id
     * As log file name hasn't been determined, we need to use a dummy one here
     */
    sprintf(nclogp->metalogpath, "%s%s_%d_%d.meta", logbase, fname, ncp->ncid, nclogp->rank);
    sprintf(nclogp->datalogpath, "%s%s_%d_%d.data", logbase, fname, ncp->ncid, nclogp->rank);
    
    /* Set log file descriptor to NULL */
    nclogp->datalog_fd = -1;
    nclogp->metalog_fd = -1;
    
    /* Misc */
    nclogp->isflushing = 0;   /* Flushing flag, set to 1 when flushing is in progress, 0 otherwise */
 
    return NC_NOERR;
}

/*
 * Initialize a new log structure
 * IN    comm:    communicator passed to ncmpi_open
 * IN    path:    path of the CDF file
 * IN    cmode:   cmode of the CDF file
 * IN    ncid:    ncid of the CDF file
 * IN    info:    path of the CDF file
 * OUT   ncdp:    Initialized log structure 
 */
int log_create(NC_Log *nclogp) {
    int i, err, flag, hintflag;
    char logbase[NC_LOG_PATH_MAX], basename[NC_LOG_PATH_MAX];
    char hint[MPI_MAX_INFO_VAL];
    char *abspath, *fname;
    DIR *logdir;
    size_t ioret, headersize;
    NC_Log_metadataheader *headerp;
    NC* ncp = (NC*)nclogp->ncp;
         
    /* Initialize metadata buffer */
    err = nclogio_log_buffer_init(&nclogp->metadata);
    if (err != NC_NOERR){
        return err;
    }
       
    /* Initialize metadata entry array */
    err = nclogio_log_sizearray_init(&nclogp->entrydatasize);
    if (err != NC_NOERR){
        return err;
    }
       
    /* Initialize metadata header */
    
    /*
     * Allocate space for metadata header
     * Header consists of a fixed size info and variable size basename
     */
    headersize = sizeof(NC_Log_metadataheader) + strlen(basename);
    if (headersize % 4 != 0){
        headersize += 4 - (headersize % 4);
    }
    headerp = (NC_Log_metadataheader*)nclogio_log_buffer_alloc(&nclogp->metadata, headersize);

    /* Fill up the metadata log header */
    memcpy(headerp->magic, NC_LOG_MAGIC, sizeof(headerp->magic));
    memcpy(headerp->format, NC_LOG_FORMAT_CDF_MAGIC, sizeof(headerp->format));
    strncpy(headerp->basename, nclogp->filepath, headersize - sizeof(NC_Log_metadataheader) + 1);
    headerp->rank_id = nclogp->rank;   /* Rank */
    headerp->num_ranks = nclogp->np;   /* Number of processes */
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
    if (!(nclogp->hints & NC_LOG_HINT_LOG_OVERWRITE)) {
        flag |= O_EXCL;
    }
    nclogp->datalog_fd = nclogp->metalog_fd = -1;
    nclogp->metalog_fd = open(nclogp->metalogpath, flag, 0744);
    if (nclogp->metalog_fd < 0) {
        err = nclogio_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
    nclogp->datalog_fd = open(nclogp->datalogpath, flag, 0744);
    if (nclogp->datalog_fd < 0) {
        err = nclogio_handle_io_error("open"); 
        DEBUG_RETURN_ERROR(err); 
    }
   
    /* Write metadata header to file
     * Write from the memory buffer to file
     */
    ioret = write(nclogp->metalog_fd, headerp, headersize);
    if (ioret < 0){
        err = nclogio_handle_io_error("write");
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
        err = nclogio_handle_io_error("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != 8){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
    nclogp->datalogsize = 8;

ERROR:;
    if (err != NC_NOERR) {
        NCI_Free(nclogp);
    }
    
    return NC_NOERR;
}

