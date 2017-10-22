#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <sys/types.h>
#include <dirent.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <../ncmpio/log.h>
#include <common.h>
#include <pnc_debug.h>
#include <linux/limits.h>

char *real_path2(char* path, char* abs_path){
    char cwd[NC_LOG_PATH_MAX];

    getwd(cwd);
    if (path[0] == '/'){
        strcpy(abs_path, path);
    }
    else{
        strcpy(abs_path, cwd);
        if (abs_path[strlen(abs_path) - 1] != '/'){
            strcat(abs_path, "/");
        }
        strcat(abs_path, path);
    }
    return abs_path;
}

int ncmpi_inq_logfile_path(int ncid, char *path, char *logbase, int rank, char *metapath, char *datapath, char *abs_path) {
    char *abspath, *fname;
    char basename[NC_LOG_PATH_MAX], abslogbase[NC_LOG_PATH_MAX];
    DIR *logdir;

    /* Determine log file name
     * Log file name is $(bufferdir)$(basename)_$(ncid)_$(rank).{meta/data}
     * filepath is absolute path to the cdf file
     */

    /* 
     * Make sure bufferdir exists 
     * NOTE: Assume upper layer already check for directory along netcdf file path
     */
    logdir = opendir(logbase);
    if (logdir == NULL) {
        /* Log base does not exist or not accessible */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    closedir(logdir);

    /* Resolve absolute path */    
    abspath = real_path2(path, abs_path);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }
    abspath = real_path2(logbase, abslogbase);
    if (abspath == NULL){
        /* Can not resolve absolute path */
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);
    }

    /* Extract file anme 
     * Search for first / charactor from the tail
     * Absolute path should always contains one '/', return error otherwise
     * We return the string including '/' for convenience
     */
    fname = strrchr(abs_path, '/');
    if (fname == NULL){
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);  
    }
    
    /* Log file path may also contain non-existing directory
     * We need to create them before we can search for usable id
     * As log file name hasn't been determined, we need to use a dummy one here
     */
    sprintf(metapath, "%s%s_%d_%d.meta", logbase, fname, ncid, rank);
    sprintf(datapath, "%s%s_%d_%d.data", logbase, fname, ncid, rank);
    
    return NC_NOERR;
}

int ncmpi_inq_logfile(int ncid, char *path, char* logbase, int rank, 
            NC_Log_file *metalog, size_t *metasize, size_t *datasize) {
    int i, err;
    int meta_fd, data_fd;
    char metapath[PATH_MAX], datapath[PATH_MAX], abspath[PATH_MAX];
    ssize_t ioret;
    struct stat logstat;
    NC_Log_metadataentry *entryp;

    err = ncmpi_inq_logfile_path(ncid, path, logbase, rank, metapath, datapath, abspath);
    if (err != NC_NOERR){
        return err;
    }
    
    meta_fd = open(metapath, O_RDONLY);
    if (meta_fd < 0){
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);  
    }
    data_fd = open(datapath, O_RDONLY);
    if (data_fd < 0){
        DEBUG_RETURN_ERROR(NC_EBAD_FILE);  
    }
    
    err = fstat(data_fd, &logstat);
    if (err < 0){
        DEBUG_RETURN_ERROR(NC_EBADLOG);
    }
    if (datasize != NULL) {
        *datasize = logstat.st_size;
    }
        
    err = fstat(meta_fd, &logstat);
    if (err < 0){
        DEBUG_RETURN_ERROR(NC_EBADLOG);
    }
    if (metasize != NULL) {
        *metasize = logstat.st_size;
    }
    
    metalog->buffer = (char*)malloc(logstat.st_size);
    ioret = read(meta_fd, metalog->buffer, logstat.st_size); 
    if (ioret < 0) {
        ioret = ncmpii_handle_io_error("read");
        if (ioret == NC_EFILE){
            ioret = NC_EREAD;
        }
        DEBUG_RETURN_ERROR(ioret);
    }
    if (ioret != logstat.st_size){
        DEBUG_RETURN_ERROR(NC_EBADLOG);
    }
    metalog->header = (NC_Log_header*)metalog->buffer;
       
    /* Check header */
    if (strncmp(NC_LOG_MAGIC, metalog->header->magic, NC_LOG_MAGIC_SIZE) != 0){
        DEBUG_RETURN_ERROR(NC_EBADLOG);
    }
    if (strncmp(NC_LOG_FORMAT_CDF_MAGIC, metalog->header->format, NC_LOG_FORMAT_SIZE) != 0){
        DEBUG_RETURN_ERROR(NC_EBADLOG);
    }
    if (strncmp(abspath, (char*)metalog->header->basename, PATH_MAX) != 0){
        DEBUG_RETURN_ERROR(NC_EBADLOG);
    }
   
    metalog->entries = (NC_Log_entry*)malloc(sizeof(NC_Log_entry) * metalog->header->num_entries);
    entryp = (NC_Log_metadataentry*)(metalog->buffer + metalog->header->entry_begin);
    for(i = 0; i < metalog->header->num_entries; i++) {
        metalog->entries[i].header = (NC_Log_entryheader*)entryp;

        metalog->entries[i].start = (MPI_Offset*)(entryp + 1);
        metalog->entries[i].count = metalog->entries[i].start + entryp->ndims;
        metalog->entries[i].stride = metalog->entries[i].count + entryp->ndims;

        entryp = (NC_Log_metadataentry*)(((char*)entryp) + entryp->esize);
    }
        
    close(meta_fd);
    close(data_fd);

    return NC_NOERR;
}

int ncmpi_free_logfile(NC_Log_file *metalog) {
    free(metalog->entries);
    free(metalog->buffer);
    return NC_NOERR;
}
