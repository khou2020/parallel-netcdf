#include <mpi.h>
#include <pnetcdf.h>
#include <dispatch.h>
#include "log.h"

int nclogio_put_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, 
                    api_kind api, nc_type itype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    
    return log_put_var(nclogp, varid, start, count, stride, imap, buf, 
                    bufcount, buftype, api, itype);
}

int nclogio_put_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, nc_type itype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return log_put_varn(nclogp->ncp, varid, num, starts, counts, buf, 
                    bufcount, buftype, itype);
}

int nclogio_iput_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, int *req, 
                    api_kind api, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    
    return log_put_var(nclogp, varid, start, count, stride, imap, buf, 
                    bufcount, buftype, api, itype);
}

int nclogio_iput_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, int *reqid, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return log_put_varn(nclogp->ncp, varid, num, starts, counts, buf, 
                    bufcount, buftype, itype);
}

int nclogio_bput_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, int *req, 
                    api_kind api, nc_type itype) {
    return nclogio_iput_var(ncdp, varid, start, count, stride, imap, buf, 
                            bufcount, buftype, req, api, itype);
}

int nclogio_bput_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, int *reqid, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogio_iput_varn(nclogp->ncp, varid, num, starts, counts, buf, 
                            bufcount, buftype, reqid, itype);
}

