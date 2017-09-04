/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifndef _NCFOO_DRIVER_H
#define _NCFOO_DRIVER_H

#include <mpi.h>
#include <pnetcdf.h>
#include <dispatch.h>

typedef struct NC_Log NC_Log; /* forward reference */

/* Log structure */
typedef struct NC_Log {
    char metalogpath[PATH_MAX];    /* path of metadata log */    
    char datalogpath[PATH_MAX];    /* path of data log */
    int rank;
    int np;
    int metalog_fd;    /* file handle of metadata log */
    int datalog_fd;    /* file handle of data log */
    int recdimid;
    size_t datalogsize;
    NC_Log_buffer metadata; /* In memory metadata buffer that mirrors the metadata log */
    NC_Log_sizearray entrydatasize;    /* Array of metadata entries */
    int isflushing;   /* If log is flushing */
    MPI_Offset total_data;
    MPI_Offset total_meta;
    MPI_Offset numrecs;
    double flush_read_time;
    double flush_replay_time;
    double flush_total_time;
    double log_write_time;
    double log_total_time;
    double total_time;
    
    int                mode;        /* file _open/_create mode */
    int                flag;        /* define/data/collective/indep mode */
    char              *path;        /* path name */
    MPI_Comm           comm;        /* MPI communicator */
    void              *ncp;         /* pointer to driver's internal object */
    struct PNC_driver *ncmpio_driver;
} NC_Log;

extern int
nclogio_create(MPI_Comm comm, const char *path, int cmode, int ncid, MPI_Info info, void **ncdp);

extern int
nclogio_open(MPI_Comm comm, const char *path, int omode, int ncid, MPI_Info info, void **ncdp);

extern int
nclogio_close(void *ncdp);

extern int
nclogio_enddef(void *ncdp);

extern int
nclogio__enddef(void *ncdp, MPI_Offset h_minfree, MPI_Offset v_align, MPI_Offset v_minfree, MPI_Offset r_align);

extern int
nclogio_redef(void *ncdp);

extern int
nclogio_sync(void *ncdp);

extern int
nclogio_abort(void *ncdp);

extern int
nclogio_set_fill(void *ncdp, int fill_mode, int *old_fill_mode);

extern int
nclogio_fill_var_rec(void *ncdp, int varid, MPI_Offset recno);

extern int
nclogio_inq(void *ncdp, int *ndimsp, int *nvarsp, int *nattsp, int *xtendimp);

extern int
nclogio_inq_misc(void *ncdp, int *pathlen, char *path, int *num_fix_varsp,
               int *num_rec_varsp, int *striping_size, int *striping_count,
               MPI_Offset *header_size, MPI_Offset *header_extent,
               MPI_Offset *recsize, MPI_Offset *put_size, MPI_Offset *get_size,
               MPI_Info *info_used, int *nreqs, MPI_Offset *usage,
               MPI_Offset *buf_size);

extern int
nclogio_sync_numrecs(void *ncdp);

extern int
nclogio_begin_indep_data(void *ncdp);

extern int
nclogio_end_indep_data(void *ncdp);

extern int
nclogio_def_dim(void *ncdp, const char *name, MPI_Offset size, int *dimidp);

extern int
nclogio_inq_dimid(void *ncdp, const char *name, int *dimidp);

extern int
nclogio_inq_dim(void *ncdp, int dimid, char *name, MPI_Offset *lengthp);

extern int
nclogio_rename_dim(void *ncdp, int dimid, const char *newname);

extern int
nclogio_inq_att(void *ncdp, int varid, const char *name, nc_type *xtypep, MPI_Offset *lenp);

extern int
nclogio_inq_attid(void *ncdp, int varid, const char *name, int *idp); 

extern int
nclogio_inq_attname(void *ncdp, int varid, int attnum, char *name);

extern int
nclogio_copy_att(void *ncdp_in, int varid_in, const char *name, void *ncdp_out, int varid_out);

extern int
nclogio_rename_att(void *ncdp, int varid, const char *name, const char *newname);

extern int
nclogio_del_att(void *ncdp, int varid, const char *name);

extern int
nclogio_get_att(void *ncdp, int varid, const char *name, void *value, MPI_Datatype itype);

extern int
nclogio_put_att(void *ncdp, int varid, const char *name, nc_type xtype, MPI_Offset nelems, const void *value, MPI_Datatype itype);

extern int
nclogio_def_var(void *ncdp, const char *name, nc_type type, int ndims, const int *dimids, int *varidp);

extern int
nclogio_def_var_fill(void *ncdp, int varid, int nofill, const void *fill_value);

extern int
nclogio_inq_var(void *ncdp, int varid, char *name, nc_type *xtypep, int *ndimsp,
               int *dimids, int *nattsp, MPI_Offset *offsetp, int *no_fill, void *fill_value);

extern int
nclogio_inq_varid(void *ncdp, const char *name, int *varid);

extern int
nclogio_rename_var(void *ncdp, int varid, const char *newname);

extern int
nclogio_get_var(void *ncdp, int varid, const MPI_Offset *start, const MPI_Offset *count, const MPI_Offset *stride, const MPI_Offset *imap, void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int reqMode);

extern int
nclogio_put_var(void *ncdp, int varid, const MPI_Offset *start, const MPI_Offset *count, const MPI_Offset *stride, const MPI_Offset *imap, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int reqMode);

extern int
nclogio_get_varn(void *ncdp, int varid, int num, MPI_Offset* const *starts, MPI_Offset* const *counts, void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int reqMode);

extern int
nclogio_put_varn(void *ncdp, int varid, int num, MPI_Offset* const *starts, MPI_Offset* const *counts, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int reqMode);

extern int
nclogio_get_vard(void *ncdp, int varid, MPI_Datatype filetype, void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int reqMode);

extern int
nclogio_put_vard(void *ncdp, int varid, MPI_Datatype filetype, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int reqMode);

extern int
nclogio_iget_var(void *ncdp, int varid, const MPI_Offset *start, const MPI_Offset *count, const MPI_Offset *stride, const MPI_Offset *imap, void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *req, int reqMode);

extern int
nclogio_iput_var(void *ncdp, int varid, const MPI_Offset *start, const MPI_Offset *count, const MPI_Offset *stride, const MPI_Offset *imap, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *req, int reqMode);

extern int
nclogio_bput_var(void *ncdp, int varid, const MPI_Offset *start, const MPI_Offset *count, const MPI_Offset *stride, const MPI_Offset *imap, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *req, int reqMode);

extern int
nclogio_iget_varn(void *ncdp, int varid, int num, MPI_Offset* const *starts, MPI_Offset* const *counts, void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *reqid, int reqMode);

extern int
nclogio_iput_varn(void *ncdp, int varid, int num, MPI_Offset* const *starts, MPI_Offset* const *counts, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *reqid, int reqMode);

extern int
nclogio_bput_varn(void *ncdp, int varid, int num, MPI_Offset* const *starts, MPI_Offset* const *counts, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *reqid, int reqMode);

extern int
nclogio_buffer_attach(void *ncdp, MPI_Offset bufsize);

extern int
nclogio_buffer_detach(void *ncdp);

extern int
nclogio_wait(void *ncdp, int num_reqs, int *req_ids, int *statuses, int reqMode);

extern int
nclogio_cancel(void *ncdp, int num_reqs, int *req_ids, int *statuses);

#endif
