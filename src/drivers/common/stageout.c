#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <mpi.h>
// NOTE: Icl is default compiler on cori, we assume compiling on cori when using icl
#ifdef __INTEL_COMPILER
#include <datawarp.h>
#endif

/*
 * Move file from bb to pfs using datawarp api
 * bb: Source file name
 * pfs: Destination file name
 * iotime: Time used to move file
 */
int ncmpi_stage_out(char* bb, char* pfs, double *iotime) {
    int ret;   
    int comp, pend, defer, fail;
    double st, et;

#ifdef __INTEL_COMPILER
    st = MPI_Wtime();
    
    /* Perform stage out */   
    ret = dw_stage_directory_out(bb, pfs, DW_STAGE_IMMEDIATE);
    if (ret != MPI_SUCCESS) {
        printf("#Error!: dw_stage_file, %d, %s\n", ret, strerror(-ret));
        return ret;
    }

    /* Wait stage out */  
    ret = dw_wait_directory_stage(bb);
    if (ret != MPI_SUCCESS) {
        printf("#Error!: dw_wait_file_stage, %d, %s\n", ret, strerror(-ret));
        return ret;
    }

    et = MPI_Wtime();

    /* Query final stage state of dw target */
    ret = dw_query_directory_stage(bb, &comp, &pend, &defer, &fail);
    if (ret != MPI_SUCCESS) {
        printf("#Error!: dw_query_file_stage, %d, %s\n", ret, strerror(-ret));
        return ret;
    }
    if (comp <= 0){
        printf("#Error!: Stagging failed\n");
        return -1;
    }
#else
    // Use cp command if not on cori, for debugging purpose
    char cmd[4096];
    printf("Not on cori, use cp\n");
    sprintf(cmd, "cp -r %s/. %s/\n", bb, pfs);
    printf("%s", cmd);
    st = MPI_Wtime();
    system(cmd);
    et = MPI_Wtime();
#endif

    // Record io time
    if (iotime != NULL){
        *iotime = et - st;
    }

    return 0;
}
