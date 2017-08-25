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
/* $Id: transpose.c 3078 2017-05-29 22:46:50Z wkliao $ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h> /* strcpy(), strncpy() */
#include <unistd.h> /* getopt() */
#include <libgen.h> /* basename() */
#include <mpi.h>
#include <pnetcdf.h>

#include <testutils.h>

#define NDIMS 3
#define LEN   2

include(`foreach.m4')dnl
include(`utils.m4')dnl

#define text char
#ifndef schar
#define schar signed char
#endif
#ifndef uchar
#define uchar unsigned char
#endif
#ifndef ushort
#define ushort unsigned short
#endif
#ifndef uint
#define uint unsigned int
#endif
#ifndef longlong
#define longlong long long
#endif
#ifndef ulonglong
#define ulonglong unsigned long long
#endif

define(`TEST_BLOCKING_PUT',dnl
`dnl
static int
blocking_put_$1(int         rank,
                int         ncid,
                int        *dimids,
                MPI_Offset *start,
                MPI_Offset *count,
                MPI_Offset *startS,
                MPI_Offset *countS,
                MPI_Offset *stride,
                MPI_Offset *startM,
                MPI_Offset *countM,
                MPI_Offset *imap,
                double     *buf)
{
    int err, nerrs=0;
    int var1_id, vara_id, vars_id, varm_id;
    int dimid, dimidsT[NDIMS];
    MPI_Offset start1;

    /* re-enter define mode, so we can add more variables */
    err = ncmpi_redef(ncid); CHECK_ERR
    err = ncmpi_inq_dimid(ncid, "nprocs", &dimid); CHECK_ERR
    err = ncmpi_def_var(ncid, "var1_$1", NC_TYPE($1),     1, &dimid, &var1_id); CHECK_ERR
    err = ncmpi_def_var(ncid, "vara_$1", NC_TYPE($1), NDIMS, dimids, &vara_id); CHECK_ERR
    err = ncmpi_def_var(ncid, "vars_$1", NC_TYPE($1), NDIMS, dimids, &vars_id); CHECK_ERR

    /* define variable with transposed file layout: ZYX -> YXZ */
    dimidsT[0] = dimids[1]; dimidsT[1] = dimids[2]; dimidsT[2] = dimids[0];
    err = ncmpi_def_var(ncid, "varm_$1", NC_TYPE($1), NDIMS, dimidsT, &varm_id); CHECK_ERR

    /* exit the define mode */
    err = ncmpi_enddef(ncid); CHECK_ERR

    /* write the whole variable in parallel */
    start1 = rank;
    err = ncmpi_put_var1_double_all(ncid, var1_id, &start1, buf); CHECK_ERR

    err = ncmpi_put_vara_double_all(ncid, vara_id, start, count, buf); CHECK_ERR

    err = ncmpi_put_vars_double_all(ncid, vars_id, startS, countS, stride, buf);
    CHECK_ERR

    err = ncmpi_put_varm_double_all(ncid, varm_id, startM, countM, NULL, imap, buf);
    CHECK_ERR

    return nerrs;
}
')dnl

foreach(`itype', (`schar, uchar, short, ushort, int, uint, long, float, double, longlong, ulonglong'), `TEST_BLOCKING_PUT(itype)')

define(`TEST_CDF_FORMAT',dnl
`dnl
    /* create a new file */
    cmode = NC_CLOBBER;
    ifelse(`$1', `NC_FORMAT_64BIT_OFFSET', `cmode |= NC_64BIT_OFFSET;',
           `$1', `NC_FORMAT_64BIT_DATA',   `cmode |= NC_64BIT_DATA;')
 
    sprintf(fname, "%s.cdf%d",filename, $1);
    err = ncmpi_create(MPI_COMM_WORLD, fname, cmode, info, &ncid);
    if (err != NC_NOERR) {
        printf("Error at line %d in %s: ncmpi_create() file %s (%s)\n",
        __LINE__,__FILE__,fname,ncmpi_strerror(err));
        MPI_Abort(MPI_COMM_WORLD, -1);
        exit(1);
    }

    /* define dimensions */
    err = ncmpi_def_dim(ncid, "nprocs", nprocs,   &dimids[0]); CHECK_ERR
    err = ncmpi_def_dim(ncid, "Z",      gsize[0], &dimids[0]); CHECK_ERR
    err = ncmpi_def_dim(ncid, "Y",      gsize[1], &dimids[1]); CHECK_ERR
    err = ncmpi_def_dim(ncid, "X",      gsize[2], &dimids[2]); CHECK_ERR
    err = ncmpi_enddef(ncid);

ifelse(`$1', `NC_FORMAT_64BIT_DATA', 
    foreach(`itype',
    (`schar, uchar, short, ushort, int, uint, long, float, double, longlong, ulonglong'),`
    _CAT(`nerrs += blocking_put_',itype)'`(rank, ncid, dimids, start, count,
             startS, countS, stride, startM, countM, imap, buf);'),
    foreach(`itype',
    (`schar, short, int, long, float, double'),`
    _CAT(`nerrs += blocking_put_',itype)'`(rank, ncid, dimids, start, count,
             startS, countS, stride, startM, countM, imap, buf);'))

    /* close the file */
    err = ncmpi_close(ncid);
    CHECK_ERR
')dnl

/*----< main() >------------------------------------------------------------*/
int main(int argc, char **argv)
{
    extern int optind;
    char filename[256], fname[512];
    int i, j, k, rank, nprocs, ncid, bufsize, err, nerrs=0, cmode;
    int psize[NDIMS], dimids[NDIMS], dim_rank[NDIMS];
    double *buf;
    MPI_Offset gsize[NDIMS], stride[NDIMS], imap[NDIMS];
    MPI_Offset start[NDIMS], count[NDIMS];
    MPI_Offset startS[NDIMS], countS[NDIMS];
    MPI_Offset startM[NDIMS], countM[NDIMS];
    MPI_Info info;

    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    if (argc > 2) {
        if (!rank) printf("Usage: %s [filename]\n",argv[0]);
        MPI_Finalize();
        return 1;
    }
    if (argc == 2) snprintf(filename, 256, "%s", argv[1]);
    else           strcpy(filename, "testfile.nc");
    MPI_Bcast(filename, 256, MPI_CHAR, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        char *cmd_str = (char*)malloc(strlen(argv[0]) + 256);
        sprintf(cmd_str, "*** TESTING C   %s for all kinds put APIs ", basename(argv[0]));
        printf("%-66s ------ ", cmd_str); fflush(stdout);
        free(cmd_str);
    }

    /* calculate number of processes along each dimension */
    for (i=0; i<NDIMS; i++) psize[i] = 0;
    MPI_Dims_create(nprocs, NDIMS, psize);

    /* for each MPI rank, find its local rank IDs along each dimension in
     * dim_rank[] */
    int lower_dims=1;
    for (i=NDIMS-1; i>=0; i--) {
        dim_rank[i] = rank / lower_dims % psize[i];
        lower_dims *= psize[i];
    }

    /* calculate gsize[], global array sizes and set arguments start and count
     * for vara APIs */
    bufsize = 1;
    for (i=0; i<NDIMS; i++) {
        gsize[i]  = (MPI_Offset)LEN * psize[i];    /* global array size */
        start[i]  = (MPI_Offset)LEN * dim_rank[i]; /* start indices */
        count[i]  = (MPI_Offset)LEN;               /* array elements */
        bufsize  *= LEN;
    }

    /* allocate buffer and initialize with contiguous numbers */
    buf = (double *) malloc(bufsize * sizeof(double));
    for (k=0; k<count[0]; k++)
    for (j=0; j<count[1]; j++)
    for (i=0; i<count[2]; i++)
        buf[k*count[1]*count[2] +
                     j*count[2] + i] = (start[0]+k)*gsize[1]*gsize[2]
                                     + (start[1]+j)*gsize[2]
                                     + (start[2]+i); // + 1000*(rank+1);

    /* set an MPI-IO hint to disable file offset alignment for fixed-size
     * variables */
    MPI_Info_create(&info);
    MPI_Info_set(info, "nc_var_align_size", "1");

    /* set arguments start, count, stride for vars APIs */
    for (i=0; i<NDIMS; i++) {
        startS[i] = dim_rank[i];
        countS[i] = gsize[i] / psize[i];
        stride[i] = psize[i];
    }

    /* ZYX -> YXZ: (this is borrowed from examples/C/transpose.c */
    imap[1] = 1; imap[0] = count[2]; imap[2] = count[1]*count[2];
    startM[0] = start[1]; startM[1] = start[2]; startM[2] = start[0];
    countM[0] = count[1]; countM[1] = count[2]; countM[2] = count[0];

    /* test CDF-1, 2, and 5 formats separately */
    TEST_CDF_FORMAT(NC_FORMAT_CLASSIC)
    TEST_CDF_FORMAT(NC_FORMAT_64BIT_OFFSET)
    TEST_CDF_FORMAT(NC_FORMAT_64BIT_DATA)

    free(buf);
    MPI_Info_free(&info);

    /* check if PnetCDF freed all internal malloc */
    MPI_Offset malloc_size, sum_size;
    err = ncmpi_inq_malloc_size(&malloc_size);
    if (err == NC_NOERR) {
        MPI_Reduce(&malloc_size, &sum_size, 1, MPI_OFFSET, MPI_SUM, 0, MPI_COMM_WORLD);
        if (rank == 0 && sum_size > 0)
            printf("heap memory allocated by PnetCDF internally has %lld bytes yet to be freed\n",
                   sum_size);
    }

    MPI_Allreduce(MPI_IN_PLACE, &nerrs, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        if (nerrs) printf(FAIL_STR,nerrs);
        else       printf(PASS_STR);
    }

    MPI_Finalize();
    return (nerrs > 0);
}

