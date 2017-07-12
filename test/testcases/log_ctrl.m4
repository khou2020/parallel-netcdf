dnl Process this m4 file to produce 'C' language file.
dnl
dnl If you see this line, you can ignore the next one.
/* Do not edit this file. It is produced from the corresponding .m4 source */
dnl
/*********************************************************************
 *
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 *
 *********************************************************************/
/* $Id: log_ctrl.c 3078 2017-07-01 22:46:50Z khou $ */

include(`foreach.m4')dnl
include(`utils.m4')dnl

define(`TEST_HINT',dnl
`dnl
    /* 
     * Flush on wait: $1
     * Flush on wait: $2
     * Flush on wait: $3 
     */
    nerr += test_hints(filename, "$1", "$2", "$3");
')dnl

define(`TEST_ENV',dnl
`dnl
    /* 
     * Flush on wait: $1
     * Flush on wait: $2
     * Flush on wait: $3 
     */
    nerr += test_env(filename, "$1", "$2", "$3");
')dnl

define(`CHECK_RET',dnl
`dnl
        for(i = 0; i < np; i++){
            if (out[i] != i + 1){
                printf("Error at line %d in %s: expecting out[%d] = %d but got %d\n", __LINE__, __FILE__, i, in, out[i]);
                nerr++;
                goto ERROR;
            }
        }
')dnl

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

#define MAXPROCESSES 1024
#define WIDTH 6
#define SINGLEPROCRANK 2
#define SINGLEPROCnp 5

int rank, np; /* Total process; Rank */

/* 
 * This function test if log io options work correctly
 * We set options via io hints
 * The test writes a 4 * np matrix M
 * Process i writes i + 1 to column i:
 * Processes writes to the column one cell at a time.
 * They call wait_all and sync after the first and second write respectly:
 * The steps are as follow:
 * put_var1 to row 0
 * wait_all
 * put_var1 to row 1
 * sync
 * put_var1 to row 2
 * begin_indep_data
 * put_var1 to row 3
 * end_indep_data
 * close
 *
 * final result should be:
 * 1 2 3 ...
 * 1 2 3 ...
 * 1 2 3 ...
 * 1 2 3 ...
 */
int test_hints(const char* filename, char* flushonwait, char* flushonsync, char* flushonread) {
    int i, j, in = rank + 1;
    int *out = NULL;
    int ret, nerr = 0;
	int ncid, varid;    /* Netcdf file id and variable id */
	int dimid[2];     /* IDs of dimension */
	MPI_Offset start[2], count[2];
	MPI_Info Info;
    
    /* Initialize file info */
    MPI_Info_create(&Info);
    MPI_Info_set(Info, "pnetcdf_log", "1");
    MPI_Info_set(Info, "pnetcdf_log_flush_on_wait", flushonwait);
    MPI_Info_set(Info, "pnetcdf_log_flush_on_sync", flushonsync);
    MPI_Info_set(Info, "pnetcdf_log_flush_on_read", flushonread);

    /* Allocate buffer */
    out = (int*)malloc(sizeof(int) * np);

    /* Create new netcdf file */
    ret = ncmpi_create(MPI_COMM_WORLD, filename, NC_CLOBBER, Info, &ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_create: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    
    /* Define dimensions and variables */
    ret = ncmpi_def_dim(ncid, "X", 4, dimid);  // X
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_def_dim: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_def_dim(ncid, "Y", np, dimid + 1);	// Y
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
    
    /* Test flush on wait
     * We write a row if ranks, call wait, and then read it back
     * If flush on wait is on, we expect to see the written value
     * Since fill mode is disalbed when log io is on, we can read anything if the data hasn't be flushed
     */
    start[0] = 0;
    start[1] = rank;
    count[0] = 1;
    count[1] = np;
    ret = ncmpi_put_var1_int_all(ncid, varid, start, &in);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_put_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_wait_all(ncid, NC_REQ_ALL, NULL, NULL);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_wait_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    memset(out, 0, sizeof(int) * np);
    ret = ncmpi_get_vara_int_all(ncid, varid, start, count, out);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_get_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    if (atoi(flushonwait)){
CHECK_RET
    }

    /* Test flush on sync
     * We write a row if ranks, call sync, and then read it back
     * If flush on sync is on, we expect to see the written value
     * Since fill mode is disalbed when log io is on, we can read anything if the data hasn't be flushed
     */
    start[0] = 1;
    ret = ncmpi_put_var1_int_all(ncid, varid, start, &in);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_put_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    ret = ncmpi_sync(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_sync: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    memset(out, 0, sizeof(int) * np);
    ret = ncmpi_get_vara_int_all(ncid, varid, start, count, out);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_get_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    if (atoi(flushonsync)){
CHECK_RET
    }
   
    /* Test flush on read
     * We write a row if ranks, and then read it back
     * If flush on read is on, we expect to see the written value
     * Since fill mode is disalbed when log io is on, we can read anything if the data hasn't be flushed
     */
    start[0] = 2;
    ret = ncmpi_put_var1_int_all(ncid, varid, start, &in);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_put_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    memset(out, 0, sizeof(int) * np);
    ret = ncmpi_get_vara_int_all(ncid, varid, start, count, out);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_get_var1_int_all: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    if (atoi(flushonread)){
CHECK_RET
    }
    
    /* Test flush on read in independent mode
     * We write a row if ranks, and then read it back
     * If flush on read is on, we expect to see the written value
     * Since fill mode is disalbed when log io is on, we can read anything if the data hasn't be flushed
     */
    ret = ncmpi_begin_indep_data(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_begin_indep_data: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    
    start[0] = 3;
    ret = ncmpi_put_var1_int(ncid, varid, start, &in);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_put_var1_int: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    memset(out, 0, sizeof(int) * np);
    ret = ncmpi_get_vara_int(ncid, varid, start, count, out);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_get_var1_int: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    /* Value from other processes may not relfect, only check value from itself */
    if (atoi(flushonread) && out[rank] != in){
        printf("Error at line %d in %s: expecting out[%d] = %d but got %d\n", __LINE__, __FILE__, rank, in, out[rank]);
        nerr++;
        goto ERROR;
    }
    
    ret = ncmpi_end_indep_data(ncid);
    if (ret != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_end_indep_data: %d\n", __LINE__, __FILE__, ret);
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

ERROR:
    
    if (out != NULL){
        free(out);
    }

    if (nerr > 0) {
        printf("Error on setting:\nFlush on wait: %s\nFlush on sync: %s\n Flush on read: %s\n", flushonwait, flushonsync, flushonread);
    }

    return nerr;
}

/* 
 * This function test if log io options work correctly
 * We set options via io environment vairable, they should overwrite hints
 * The test writes a 3 * np matrix M
 * Process i writes i + 1 to column i:
 * Processes writes to the column one cell at a time.
 * They call wait_all and sync after the first and second write respectly:
 * The steps are as follow:
 * put_var1 to row 0
 * wait_all
 * put_var1 to row 1
 * sync
 * put_var1 to row 2
 * close
 *
 * final result should be:
 * 1 2 3 ...
 * 1 2 3 ...
 * 1 2 3 ...
 */
int test_env(const char* filename, char* flushonwait, char* flushonsync, char* flushonread) {
    int ret, nerr = 0;
    char env[1024];

    /* Set environment variable */
    sprintf(env, "pnetcdf_log=1;pnetcdf_log_flush_on_wait=%s;pnetcdf_log_flush_on_sync=%s;pnetcdf_log_flush_on_read=%s", flushonwait, flushonsync, flushonread);
    ret = setenv("PNETCDF_HINTS", env, 1);
    if (ret != 0) {
        printf("Error at line %d in %s: setenv: %d\n", __LINE__, __FILE__, ret);
        nerr++;
        goto ERROR;
    }
    
    /* Run test, the hints should be overriten by the environment variable */
    nerr += test_hints(filename, flushonwait, flushonsync, flushonread);

ERROR:;

    return nerr;
}

int main(int argc, char* argv[]) {
    int ret, nerr = 0;
    char filename[PATH_MAX];
    
    /* Initialize MPI */
    MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &np);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    if (argc > 2) {
        if (!rank) printf("Usage: %s [filename]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    if (argc == 2) snprintf(filename, PATH_MAX, "%s", argv[1]);
    else           strcpy(filename, "testfile.nc");
	
    if (rank == 0) {
        char *cmd_str = (char*)malloc(strlen(argv[0]) + 256);
        sprintf(cmd_str, "*** TESTING C   %s for checking log io options functionality", basename(argv[0]));
		printf("%-66s ------ ", cmd_str); fflush(stdout);
		free(cmd_str);
	}
        
    /* Perform test with hints */

foreach(`x', (`0, 1'), `foreach(`y', (`0, 1'), `foreach(`z', (`0, 1'), `TEST_HINT(x, y, z)')')')dnl

    /* Perform test with environment variable */
 
foreach(`x', (`0, 1'), `foreach(`y', (`0, 1'), `foreach(`z', (`0, 1'), `TEST_ENV(x, y, z)')')')dnl
 
	/* check if PnetCDF freed all internal malloc */
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
