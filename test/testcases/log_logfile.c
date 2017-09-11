/* Do not edit this file. It is produced from the corresponding .m4 source */
/*********************************************************************
 *
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 *
 *********************************************************************/
/* $Id: log_ctrl.c 3078 2017-07-01 22:46:50Z khou $ */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>
#include <libgen.h> /* basename() */
#include <string.h> /* strcpy(), memset() */
#include <testutils.h>
#include <pnetcdf.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <linux/limits.h>
#include <../../src/drivers/ncmpio/nc.h>
#include <../../src/drivers/ncmpio/log.h>

#define MAXPROCESSES 1024
#define WIDTH 6
#define SINGLEPROCRANK 2
#define SINGLEPROCnp 5

/* 
 * This function test if log file can be successfully created
 * It create a 1-D variable of size np and check if corresponding log file is created
 * Each process then write it's own rank to the variable
 * Process n writes to n-th cell
 * It opens the log file again to see if size is correct
 */
int main(int argc, char* argv[]) {
    int ret, nerr = 0;
    int meta_fd, data_fd;
    int np, rank;
    char *pathret, *fname;
    char metalogpath[PATH_MAX];
    char datalogpath[PATH_MAX];
    char filename[PATH_MAX];
    char absfilename[PATH_MAX];
    char abslogbase[PATH_MAX];
    char logbase[PATH_MAX];
    //struct stat metastat, datastat;
	size_t metasize, datasize, expmetasize;
    int ncid, varid, dimid, buf;    /* Netcdf file id and variable id */
    MPI_Offset start;
    MPI_Info Info;
    NC_Log_file log;

    /* Initialize MPI */
    MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &np);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    if (argc > 3) {
        if (!rank) printf("Usage: %s [filename]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    if (argc > 1){
        snprintf(filename, PATH_MAX, "%s", argv[1]);
    }
    else {
        strcpy(filename, "testfile.nc");
	}
    if (argc > 2){
        snprintf(logbase, PATH_MAX, "%s", argv[2]);
    }
    else {
        strcpy(logbase, ".");
    }    

    /* Resolve absolute path */ 
    pathret = realpath(logbase, abslogbase);
    if (pathret == NULL){
        printf("Error at line %d in %s: Can not resolve log base path\n", __LINE__, __FILE__);
        nerr++;
        goto ERROR;
    }  
    
    /* 
     * Extract file anme 
     * Search for first / charactor from the tail
     */
    fname = strrchr(filename, '/');
    if (fname == NULL){
        /* We have relative path with only the file name */
        fname = filename;
    }
    
    /* Determine log file name */
    if (fname == filename){
        sprintf(metalogpath, "%s/%s_%d_%d.meta", abslogbase, fname, ncid, rank);
        sprintf(datalogpath, "%s/%s_%d_%d.data", abslogbase, fname, ncid, rank);
    }
    else {
        sprintf(metalogpath, "%s%s_%d_%d.meta", abslogbase, fname, ncid, rank);
        sprintf(datalogpath, "%s%s_%d_%d.data", abslogbase, fname, ncid, rank);
    }

    if (rank == 0) {
        char *cmd_str = (char*)malloc(strlen(argv[0]) + 256);
        sprintf(cmd_str, "*** TESTING C   %s for checking log io options functionality", basename(argv[0]));
		printf("%-66s ------ ", cmd_str); fflush(stdout);
		free(cmd_str);
	}
    
    /* Determine test file name */

    /* Initialize file info */
    MPI_Info_create(&Info);
    MPI_Info_set(Info, "pnetcdf_bb", "enable");
    MPI_Info_set(Info, "pnetcdf_bb_dirname", abslogbase);   
    
    /* Create new netcdf file */
    ret = ncmpi_create(MPI_COMM_WORLD, filename, NC_CLOBBER, Info, &ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_create: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
     
    /* Define dimensions and variables */
    ret = ncmpi_def_dim(ncid, "X", np, &dimid);  // X
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_def_dim: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_def_var(ncid, "V", NC_INT, 1, &dimid, &varid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_def_var: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }

    /* Delete old log file */
    remove(metalogpath);
    remove(datalogpath);
     
    /* Switch to data mode */
    ret = ncmpi_enddef(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_enddef: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
        
    /* 
     * Resolve absolute path
     * We need absolute file name to calculate size of metadata header size
     */ 
    pathret = realpath(filename, absfilename);
    if (pathret == NULL){
        printf("Error at line %d in %s: Can not resolve file name\n", __LINE__, __FILE__);
        nerr++;
        goto ERROR;
    } 
    
    /* Check log file status */
    ret = ncmpi_inq_logfile(ncid, filename, logbase, rank, &log, &metasize, &datasize);
    if (ret != NC_NOERR){
        printf("Error at line %d in %s: ncmpi_inq_logfile: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    expmetasize = sizeof(NC_Log_metadataheader) + strlen(absfilename);
    if (expmetasize % 4 != 0){
        expmetasize += 4 - (expmetasize % 4);
    }
    if (metasize != expmetasize){
        printf("Error at line %d in %s: expecting metadata log size = %d but got %d\n", __LINE__, __FILE__, expmetasize, metasize);
        nerr++;
        goto ERROR;
    }
    if (datasize != 8){
        printf("Error at line %d in %s: expecting data log size = %d but got %d\n", __LINE__, __FILE__, 8, datasize);
        nerr++;
        goto ERROR;
    }
    if (log.header->num_entries != 0){
        printf("Error at line %d in %s: expecting num_entries = %d but got %d\n", __LINE__, __FILE__, 1, log.header->num_entries);
        nerr++;
        goto ERROR;
    }
    if (log.header->num_ranks != np){
        printf("Error at line %d in %s: expecting np = %d but got %d\n", __LINE__, __FILE__, np, log.header->num_ranks);
        nerr++;
        goto ERROR;
    }
    if (log.header->max_ndims != 1){
        printf("Error at line %d in %s: expecting max_ndims = %d but got %d\n", __LINE__, __FILE__, 1, log.header->max_ndims);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_free_logfile(&log);
    if (ret != NC_NOERR){
        printf("Error at line %d in %s: ncmpi_free_logfile: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
 
    /* Write rank to variable */
    start = rank;
    ret = ncmpi_put_var1_int_all(ncid, varid, &start, &rank);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_put_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    
    /* Check log file status */
    ret = ncmpi_inq_logfile(ncid, filename, logbase, rank, &log, &metasize, &datasize);
    if (ret != NC_NOERR){
        printf("Error at line %d in %s: ncmpi_inq_logfile: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    expmetasize = sizeof(NC_Log_metadataheader) + strlen(absfilename) + sizeof(NC_Log_metadataentry) + sizeof(MPI_Offset) * 3;
    if (expmetasize % 4 != 0){
        expmetasize += 4 - (expmetasize % 4);
    }
    if (metasize != expmetasize){
        printf("Error at line %d in %s: expecting metadata log size = %d but got %d\n", __LINE__, __FILE__, expmetasize, metasize);
        nerr++;
        goto ERROR;
    }
    if (datasize != 8 + sizeof(int)){
        printf("Error at line %d in %s: expecting data log size = %d but got %d\n", __LINE__, __FILE__, 8 + sizeof(int), datasize);
        nerr++;
        goto ERROR;
    }
    if (log.header->num_entries != 1){
        printf("Error at line %d in %s: expecting num_entries = %d but got %d\n", __LINE__, __FILE__, 1, log.header->num_entries);
        nerr++;
        goto ERROR;
    }
    if (log.header->num_ranks != np){
        printf("Error at line %d in %s: expecting np = %d but got %d\n", __LINE__, __FILE__, np, log.header->num_ranks);
        nerr++;
        goto ERROR;
    }
    if (log.header->max_ndims != 1){
        printf("Error at line %d in %s: expecting max_ndims = %d but got %d\n", __LINE__, __FILE__, 1, log.header->max_ndims);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].header->api_kind != NC_LOG_API_KIND_VARA){
        printf("Error at line %d in %s: expecting api_kind = %d but got %d\n", __LINE__, __FILE__, NC_LOG_API_KIND_VARA, log.entries[0].header->api_kind);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].header->itype != NC_LOG_TYPE_INT){
        printf("Error at line %d in %s: expecting itype = %d but got %d\n", __LINE__, __FILE__, NC_LOG_TYPE_INT, log.entries[0].header->itype);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].header->varid != varid){
        printf("Error at line %d in %s: expecting varid = %d but got %d\n", __LINE__, __FILE__, varid, log.entries[0].header->varid);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].header->ndims != 1){
        printf("Error at line %d in %s: expecting ndims = %d but got %d\n", __LINE__, __FILE__, 1, log.entries[0].header->ndims);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].header->data_off != 8){
        printf("Error at line %d in %s: expecting data_off = %d but got %d\n", __LINE__, __FILE__, 8, log.entries[0].header->data_off);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].header->data_len != sizeof(int)){
        printf("Error at line %d in %s: expecting data_len = %d but got %d\n", __LINE__, __FILE__, sizeof(int), log.entries[0].header->data_len);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].start[0] != rank){
        printf("Error at line %d in %s: expecting start = %d but got %d\n", __LINE__, __FILE__, rank, log.entries[0].start[0]);
        nerr++;
        goto ERROR;
    }
    if (log.entries[0].count[0] != 1){
        printf("Error at line %d in %s: expecting count = %d but got %d\n", __LINE__, __FILE__, 1, log.entries[0].count[0]);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_free_logfile(&log);
    if (ret != NC_NOERR){
        printf("Error at line %d in %s: ncmpi_free_logfile: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }

    /* Close file */
    ret = ncmpi_close(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_close: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
   
    /* Check log file, should fail*/
    ret = ncmpi_inq_logfile(ncid, filename, logbase, rank, &log, &metasize, &datasize);
    if (ret != NC_EBAD_FILE){
        printf("Error at line %d in %s: ncmpi_inq_logfile: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
	
    /* Check if PnetCDF freed all internal malloc */
    MPI_Offset malloc_size, sum_size;
    ret = ncmpi_inq_malloc_size(&malloc_size);
    if (ret == NC_NOERR) {
        MPI_Reduce(&malloc_size, &sum_size, 1, MPI_OFFSET, MPI_SUM, 0, MPI_COMM_WORLD);
        if (rank == 0 && sum_size > 0)
            printf("heap memory allocated by PnetCDF internally has %lld bytes yet to be freed\n", sum_size);
    }
    
ERROR:
    MPI_Allreduce(MPI_IN_PLACE, &nerr, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        if (nerr) printf(FAIL_STR, nerr);
        else       printf(PASS_STR);
    }

    MPI_Finalize();
    
    return nerr > 0;
}
