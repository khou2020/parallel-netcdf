/* Do not edit this file. It is produced from the corresponding .m4 source */
/*********************************************************************
 *
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 *
 *********************************************************************/
/* $Id: log_ctrl.c 3078 2017-07-01 22:46:50Z khou $ */






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

#define MAXPROCESSES 1024
#define WIDTH 6
#define SINGLEPROCRANK 2
#define SINGLEPROCnp 5

int rank, np; /* Total process; Rank */

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
    char *pathret, *fname;
    char metalogpath[PATH_MAX];
    char datalogpath[PATH_MAX];
    char filename[PATH_MAX];
    char abslogbase[PATH_MAX];
    char logbase[PATH_MAX];
    struct stat metastat, datastat;
	int ncid, varid, dimid, buf;    /* Netcdf file id and variable id */
    MPI_Offset start;
    MPI_Info Info;
 
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
        /* We have relative path with only file name */
        fname = filename;
    }
    
    /* Determine log file name */
    sprintf(metalogpath, "%s%s_%d_%d.meta", abslogbase, fname, ncid, rank);
    sprintf(datalogpath, "%s%s_%d_%d.data", abslogbase, fname, ncid, rank);

    if (rank == 0) {
        char *cmd_str = (char*)malloc(strlen(argv[0]) + 256);
        sprintf(cmd_str, "*** TESTING C   %s for checking log io options functionality", basename(argv[0]));
		printf("%-66s ------ ", cmd_str); fflush(stdout);
		free(cmd_str);
	}
    
    /* Determine test file name */

    /* Initialize file info */
    MPI_Info_create(&Info);
    MPI_Info_set(Info, "pnetcdf_log", "1");
    MPI_Info_set(Info, "pnetcdf_log_base", abslogbase);   
    
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
    
    /* Open log file */
    meta_fd = open(metalogpath, O_RDONLY);
    if (meta_fd < 0){
        printf("Error at line %d in %s: open: %d\n", __LINE__, __FILE__, meta_fd);
        nerr++;
        goto ERROR;
    }
    data_fd = open(datalogpath, O_RDONLY);
    if (meta_fd < 0){
        printf("Error at line %d in %s: open: %d\n", __LINE__, __FILE__, data_fd);
        nerr++;
        goto ERROR;
    }

    /* Querying file status */
    ret = fstat(meta_fd, &metastat);
    if (ret < 0){
        printf("Error at line %d in %s: fstat: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = fstat(data_fd, &datastat);
    if (ret < 0){
        printf("Error at line %d in %s: fstat: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
 
    /* Check log file size */
    if (metastat.st_size != sizeof(NC_Log_metadataheader)){
        printf("Error at line %d in %s: expecting metadata log size = %d but got %d\n", __LINE__, __FILE__, sizeof(NC_Log_metadataheader), metastat.st_size);
        nerr++;
        goto ERROR;
    }
    if (datastat.st_size != 8){
        printf("Error at line %d in %s: expecting data log size = %d but got %d\n", __LINE__, __FILE__, 8, datastat.st_size);
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
    
    /* Querying file status */
    ret = fstat(meta_fd, &metastat);
    if (ret < 0){
        printf("Error at line %d in %s: fstat: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = fstat(data_fd, &datastat);
    if (ret < 0){
        printf("Error at line %d in %s: fstat: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
 
    /* Check log file size */
    if (metastat.st_size != sizeof(NC_Log_metadataheader) + sizeof(NC_Log_metadataentry) + sizeof(MPI_Offset) * 3){
        printf("Error at line %d in %s: expecting metadata log size = %d but got %d\n", __LINE__, __FILE__, sizeof(NC_Log_metadataheader) + sizeof(NC_Log_metadataentry) + sizeof(MPI_Offset) * 3, metastat.st_size);
        nerr++;
        goto ERROR;
    }
    if (datastat.st_size != 8 + sizeof(int)){
        printf("Error at line %d in %s: expecting data log size = %d but got %d\n", __LINE__, __FILE__, 8 + sizeof(int), datastat.st_size);
        nerr++;
        goto ERROR;
    }
    
    /* Close log file */
    close(meta_fd);
    close(data_fd);

    /* Close file */
    ret = ncmpi_close(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_close: %d\n", __LINE__, __FILE__, ret);
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
