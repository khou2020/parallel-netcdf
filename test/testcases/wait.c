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
    int np, rank;
    int dimid[2], reqid[2], stat[2];
    char filename[PATH_MAX];
    char logbase[PATH_MAX];
    int ncid, varid, buf;    /* Netcdf file id and variable id */
    MPI_Offset start[2];
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
    
    if (rank == 0) {
        char *cmd_str = (char*)malloc(strlen(argv[0]) + 256);
        sprintf(cmd_str, "*** TESTING C   %s for checking log io options functionality", basename(argv[0]));
		printf("%-66s ------ ", cmd_str); fflush(stdout);
		free(cmd_str);
	}
    
    /* Determine test file name */

    /* Create new netcdf file */
    ret = ncmpi_create(MPI_COMM_WORLD, filename, NC_CLOBBER, MPI_INFO_NULL, &ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_create: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
     
    /* Define dimensions and variables */
    ret = ncmpi_def_dim(ncid, "X", np, dimid);  // X
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_def_dim: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_def_dim(ncid, "Y", 2, dimid + 1);  // X
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_def_dim: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_def_var(ncid, "V", NC_INT, 2, dimid, &varid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_def_var: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
         
    /* Switch to data mode */
    ret = ncmpi_enddef(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_enddef: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
         
    /* Write rank to variable */
    start[0] = rank;
    start[1] = 0;
    ret = ncmpi_iput_var1_int(ncid, varid, start, &rank, reqid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_iput_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    
    /* Call sync, triggers a flush */
    ret = ncmpi_sync(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_sync: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
 
    /* Write rank to variable */
    start[0] = rank;
    start[1] = 1;
    ret = ncmpi_iput_var1_int(ncid, varid, start, &rank, reqid + 1);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_iput_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    
    /* Wait for nun-blocking operation */
    ret = ncmpi_wait_all(ncid, 2, reqid, stat);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_wait_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    if (stat[0] != NC_NOERR){
        printf("Error at line %d in %s: stat[0]: %d\n", __LINE__, __FILE__, stat[0]);
        nerr++;
        goto ERROR;
    }
    if (stat[1] != NC_NOERR){
        printf("Error at line %d in %s: stat[1]: %d\n", __LINE__, __FILE__, stat[1]);
        nerr++;
        goto ERROR;
    }
    
    /* Read from variable */
    start[0] = rank;
    start[1] = 0;
    ret = ncmpi_get_var1_int_all(ncid, varid, start, &buf);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_get_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    if (buf != rank){
        printf("Error at line %d in %s: expecting buf = %d but got %d\n", __LINE__, __FILE__, rank, buf);
        nerr++;
    }
 
    /* Read from variable */
    start[0] = rank;
    start[1] = 1;
    ret = ncmpi_get_var1_int_all(ncid, varid, start, &buf);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_get_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    if (buf != rank){
        printf("Error at line %d in %s: expecting buf = %d but got %d\n", __LINE__, __FILE__, rank, buf);
        nerr++;
    }
 
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
