/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifndef _NCLOGIO_DISPATCH_H
#define _NCLOGIO_DISPATCH_H

#include <mpi.h>
#include <pnetcdf.h>
#include <dispatch.h>
#include "log.h"

int nclogio_create(MPI_Comm comm, const char *path, int cmode, int ncid, 
                    MPI_Info info, void **ncdp) {
    int ret;
    NC_Log *nclogp = (NC_Log*)malloc(sizeof(NC_Log));
    nclogp->ncmpio_dispatcher = ncmpio_inq_dispatcher();

    ret = nclogp->ncmpio_dispatcher->create(comm, path, cmode, 
                    ncid, info, &nclogp->ncp);
    if (ret != NC_NOERR){
        return ret;
    }

    *ncdp = nclogp;
    
    return NC_NOERR;
}

int nclogio_open(MPI_Comm comm, const char *path, int omode, int ncid, 
                    MPI_Info info, void **ncdp) {
    int ret;
    NC_Log *nclogp = (NC_Log*)malloc(sizeof(NC_Log));
    nclogp->ncmpio_dispatcher = ncmpio_inq_dispatcher();
    
    ret = nclogp->ncmpio_dispatcher->open(comm, path, omode, ncid, 
                    info, &nclogp->ncp);
    if (ret != NC_NOERR){
        return ret;
    }

    *ncdp = nclogp;
    
    return NC_NOERR;
}

int nclogio_close(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->close(nclogp->ncp);
}

int nclogio_enddef(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->enddef(nclogp->ncp);
}

int nclogio__enddef(void *ncdp, MPI_Offset h_minfree, MPI_Offset v_align, 
                MPI_Offset v_minfree, MPI_Offset r_align) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->_enddef(nclogp->ncp, h_minfree, 
                v_align, v_minfree, r_align);
}

int nclogio_redef(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->redef(nclogp->ncp);
}

int nclogio_sync(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->sync(nclogp->ncp);
}

int nclogio_abort(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->abort(nclogp->ncp);
}

int nclogio_set_fill(void *ncdp, int fill_mode, int *old_fill_mode) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->set_fill(nclogp->ncp, fill_mode, 
                                                        old_fill_mode);
}

int nclogio_fill_var_rec(void *ncdp, int varid, MPI_Offset recno) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->fill_rec(nclogp->ncp, varid, 
                                                            recno);
}

int nclogio_inq(void *ncdp, int *ndimsp, int *nvarsp, int *nattsp, 
                int *xtendimp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq(nclogp->ncp, ndimsp, nvarsp, 
                                                    nattsp, xtendimp);
}


int nclogio_inq_misc(void *ncdp, int *pathlen, char *path, int *num_fix_varsp,
                int *num_rec_varsp, int *striping_size, int *striping_count,
                MPI_Offset *header_size, MPI_Offset *header_extent,
                MPI_Offset *recsize, MPI_Offset *put_size, MPI_Offset *get_size,
                MPI_Info *info_used, int *nreqs, MPI_Offset *usage,
                MPI_Offset *buf_size) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_misc(nclogp->ncp, pathlen, 
                path, num_fix_varsp, num_rec_varsp, striping_size, 
                striping_count, header_size, header_extent, recsize, put_size, 
                get_size, info_used, nreqs, usage, buf_size);
}

int nclogio_sync_numrecs(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->sync_numrecs(nclogp->ncp);
}

int nclogio_begin_indep_data(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->begin_indep_data(nclogp->ncp);
}

int nclogio_end_indep_data(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->end_indep_data(nclogp->ncp);
}

int nclogio_def_dim(void *ncdp, const char *name, MPI_Offset size, 
                    int *dimidp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->def_dim(nclogp->ncp, name, size, 
                                                    dimidp);
}

int nclogio_inq_dimid(void *ncdp, const char *name, int *dimidp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_dimid(nclogp->ncp, name, dimidp);
}

int nclogio_inq_dim(void *ncdp, int dimid, char *name, MPI_Offset *lengthp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_dim(nclogp->ncp, dimid, 
                                                        name, lengthp);
}

int nclogio_rename_dim(void *ncdp, int dimid, const char *newname) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->rename_dim(nclogp->ncp, dimid, 
                                                        newname);
}

int nclogio_inq_att(void *ncdp, int varid, const char *name, nc_type *xtypep, 
                    MPI_Offset *lenp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_att(nclogp->ncp, varid, 
                                                        name, xtypep, lenp);
}

int nclogio_inq_attid(void *ncdp, int varid, const char *name, int *idp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_attid(nclogp->ncp, varid, 
                                                        name, idp);
}

int nclogio_inq_attname(void *ncdp, int varid, int attnum, char *name) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_attname(nclogp->ncp, varid, 
                                                        attnum, name);
}

int nclogio_copy_att(void *ncdp_in, int varid_in, const char *name, 
                    void *ncdp_out, int varid_out) {
    NC_Log *nclogp_in = (NC_Log*)ncdp_in;   
    NC_Log *nclogp_out = (NC_Log*)ncdp_out;   
    return nclogp_in->ncmpio_dispatcher->copy_att(nclogp_in->ncp, 
                            varid_in, name, nclogp_out->ncp, varid_out);
}

int nclogio_rename_att(void *ncdp, int varid, const char *name, 
                        const char *newname) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->rename_att(nclogp->ncp, varid, 
                                                        name, newname);
}

int nclogio_del_att(void *ncdp, int varid, const char *name) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->del_att(nclogp->ncp, varid, 
                                                        name);
}

int nclogio_get_att(void *ncdp, int varid, const char *name, void *value, 
                    nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->get_att(nclogp->ncp, varid, 
                                                        name, value, itype);
}

int nclogio_put_att(void *ncdp, int varid, const char *name, nc_type xtype, 
                    MPI_Offset nelems, const void *value, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->put_att(nclogp->ncp, varid, name, xtype, 
                                              nelems, value, itype);
}

int nclogio_def_var(void *ncdp, const char *name, nc_type type, int ndims, 
                    const int *dimids, int *varidp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->def_var(nclogp->ncp, name, type, 
                                                    ndims, dimids, varidp);
}

int nclogio_def_var_fill(void *ncdp, int varid, int nofill, 
                        const void *fill_value) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->def_var_fill(nclogp->ncp, varid, 
                                                        nofill, fill_value);
}

int nclogio_inq_var(void *ncdp, int varid, char *name, nc_type *xtypep, 
                    int *ndimsp, int *dimids, int *nattsp, 
                    MPI_Offset *offsetp, int *no_fill, void *fill_value) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_var(nclogp->ncp, varid, 
                                        name, xtypep, ndimsp, dimids, 
                                        nattsp, offsetp, no_fill, fill_value);
}

int nclogio_inq_varid(void *ncdp, const char *name, int *varid) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->inq_varid(nclogp->ncp, name, 
                                                        varid);
}

int nclogio_rename_var(void *ncdp, int varid, const char *newname) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->rename_var(nclogp->ncp, varid, 
                                                        newname);
}

int nclogio_get_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride,
                    const MPI_Offset *imap, void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, api_kind api, nc_type itype, 
                    int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->get_var(nclogp->ncp, varid, start, 
                    count, stride, imap, buf, bufcount, buftype, api, itype, 
                    io_method);
}

int nclogio_put_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, 
                    api_kind api, nc_type itype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->put_var(nclogp->ncp, varid, 
                    start, count, stride, imap, buf, bufcount, buftype, api, 
                    itype, io_method);
}

int nclogio_get_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    void *buf, MPI_Offset bufcount, MPI_Datatype buftype, 
                    nc_type itype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->get_varn(nclogp->ncp, varid, 
                    num, starts, counts, buf, bufcount, buftype, itype, 
                    io_method);
}

int nclogio_put_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, nc_type itype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->put_varn(nclogp->ncp, varid, 
                    num, starts, counts, buf, bufcount, buftype, itype, 
                    io_method);
}

int nclogio_get_vard(void *ncdp, int varid, MPI_Datatype filetype, void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->get_vard(nclogp->ncp, varid, 
                    filetype, buf, bufcount, buftype, io_method);
}

int nclogio_put_vard(void *ncdp, int varid, MPI_Datatype filetype, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->put_vard(nclogp->ncp, varid, 
                    filetype, buf, bufcount, buftype, io_method);
}

int nclogio_iget_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, int *req, api_kind api, 
                    nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->iget_var(nclogp->ncp, varid, start, 
                    count, stride, imap, buf, bufcount, buftype, req, api, 
                    itype);
}

int nclogio_iput_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, int *req, 
                    api_kind api, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->iput_var(nclogp->ncp, varid, 
                    start, count, stride, imap, buf, bufcount, buftype, req, 
                    api, itype);
}

int nclogio_bput_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, int *req, 
                    api_kind api, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->bput_var(nclogp->ncp, varid, 
                    start, count, stride, imap, buf, bufcount, buftype, req, 
                    api, itype);
}

int nclogio_iget_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    void *buf, MPI_Offset bufcount, MPI_Datatype buftype, 
                    int *reqid, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->iget_varn(nclogp->ncp, varid, 
                    num, starts, counts, buf, bufcount, buftype, reqid, itype);
}

int nclogio_iput_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, int *reqid, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->iput_varn(nclogp->ncp, varid, 
                    num, starts, counts, buf, bufcount, buftype, reqid, itype);
}

int nclogio_bput_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, int *reqid, nc_type itype) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->bput_varn(nclogp->ncp, varid, 
                    num, starts, counts, buf, bufcount, buftype, reqid, itype);
}

int nclogio_buffer_attach(void *ncdp, MPI_Offset bufsize) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->buffer_attach(nclogp->ncp, bufsize);
}

int nclogio_buffer_detach(void *ncdp) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->buffer_detach(nclogp->ncp);
}

int nclogio_wait(void *ncdp, int num_reqs, int *req_ids, int *statuses, 
                    int io_method) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->wait(nclogp->ncp, num_reqs, req_ids, 
                    statuses, io_method);
}

int nclogio_cancel(void *ncdp, int num_reqs, int *req_ids, int *statuses) {
    NC_Log *nclogp = (NC_Log*)ncdp;   
    return nclogp->ncmpio_dispatcher->cancel(nclogp->ncp, num_reqs, 
                    req_ids, statuses);
}

#endif
