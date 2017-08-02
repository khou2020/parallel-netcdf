/*
 *  Copyright (C) 2015, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>  /* strcasecmp() */
#include <mpi.h>

#include <pnc_debug.h>
#include "nc.h"
#include "ncmpidtype.h"
#include "ncx.h"

/*----< ncmpii_sanity_check() >----------------------------------------------*/
/* check the following errors and in that precedence.
 * NC_EBADID, NC_EPERM, NC_EINDEFINE, NC_EINDEP/NC_ENOTINDEP, NC_ENOTVAR,
 * NC_ECHAR, NC_EINVALCOORDS, NC_EEDGE, NC_ESTRIDE, NC_EINVAL.
 */
int ncmpii_sanity_check(NC                *ncp,
                        int               varid,
                        const MPI_Offset *start,
                        const MPI_Offset *count,
                        const MPI_Offset *stride,
                        const MPI_Offset  bufcount,
                        MPI_Datatype      buftype,  /* internal datatype */
                        api_kind          api,
                        int               isFlexibleAPI,
                        int               mustInDataMode,
                        int               rw_flag,
                        int               io_method,
                        NC_var          **varp)  /* OUT */
{
    /* all errors detected here are fatal, must return immediately */
    int i, firstDim, err;
    
    /* Replay if log is enabled */
    if (rw_flag == READ_REQ){
        if (ncp->nclogp != NULL){
            /* Flush the log file if flag is on */
            if (ncp->loghints & NC_LOG_HINT_FLUSH_ON_READ ){
                err = ncmpii_log_flush(ncp);    
                if (err != NC_NOERR){
                    DEBUG_ASSIGN_ERROR(err, NC_EPERM)
                    goto fn_exit;
                }
            }
        }
    }

    /* check file write permission if this is write request */
    if (rw_flag == WRITE_REQ && NC_readonly(ncp)) {
        DEBUG_ASSIGN_ERROR(err, NC_EPERM)
        goto fn_exit;
    }

    /* if this call must be made in data mode, check if currently is in define
     * mode */
    if (mustInDataMode && NC_indef(ncp)) {
        DEBUG_ASSIGN_ERROR(err, NC_EINDEFINE)
        goto fn_exit;
    }

    if (io_method != NONBLOCKING_IO) { /* for blocking APIs */
        /* check if in the right collective or independent mode and initialize
         * MPI file handlers */
        err = ncmpii_check_mpifh(ncp, io_method);
        if (err != NC_NOERR) goto fn_exit;
    }

    /* check if varid is valid (check NC_ENOTVAR) */
    err = ncmpii_NC_lookupvar(ncp, varid, varp);
    if (err != NC_NOERR) goto fn_exit;

    /* check NC_ECHAR */
    if (isFlexibleAPI) {
        /* when buftype == MPI_DATATYPE_NULL, bufcount is ignored and this API
         * assumes argument buf's data type matches the data type of variable
         * defined in the file - no data conversion will be done.
         */
        if (buftype != MPI_DATATYPE_NULL) {
            int isderived, el_size, buftype_is_contig;
            MPI_Datatype ptype;
            MPI_Offset   bnelems=0;

            err = ncmpii_dtype_decode(buftype, &ptype, &el_size, &bnelems,
                                      &isderived, &buftype_is_contig);
            if (err != NC_NOERR) goto fn_exit;

            err = NCMPII_ECHAR((*varp)->type, ptype);
            if (err != NC_NOERR) goto fn_exit;
        }
        /* else case types are matched */
    }
    else {
        err = NCMPII_ECHAR((*varp)->type, buftype);
        if (err != NC_NOERR) goto fn_exit;
    }

    /* for flexible APIs, bufcount cannot be negative */
    if (isFlexibleAPI && bufcount < 0) {
        DEBUG_ASSIGN_ERROR(err, NC_EINVAL)
        goto fn_exit;
    }

    if ((*varp)->ndims == 0) { /* scalar variable: ignore start/count/stride */
        err = NC_NOERR;
        goto fn_exit;
    }

    if (api <= API_VAR) { /* var/varn/vard APIs, start/count/stride are NULL */
        err = NC_NOERR;
        goto fn_exit;
    }

    /* Now only check var1, vara, vars, and varm APIs */

    /* Check NC_EINVALCOORDS
     * for API var1/vara/vars/varm, start cannot be NULL, except for scalars */
    if (start == NULL) {
        DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
        goto fn_exit;
    }

    firstDim = 0;
    if (IS_RECVAR(*varp)) {
        if (start[0] < 0) { /* no negative value */
            DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
            goto fn_exit;
        }

        if (ncp->format < 5 &&    /* not CDF-5 */
            start[0] > X_UINT_MAX) { /* sanity check */
            DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
            goto fn_exit;
        }

        /* for record variable, [0] is the NC_UNLIMITED dimension */
        if (rw_flag == READ_REQ) {
            /* read cannot go beyond current numrecs */
#ifdef RELAX_COORD_BOUND
            if (start[0] >  ncp->numrecs) {
                DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
                goto fn_exit;
            }
            if (start[0] == ncp->numrecs) {
                if (api == API_VAR1) {
                    /* for var1 APIs, count[0] is considered of 1 */
                    DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
                    goto fn_exit;
                }
                else if (count != NULL && count[0] > 0) {
                    DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
                    goto fn_exit;
                }
            }
#else
            if (start[0] >= ncp->numrecs) {
                DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
                goto fn_exit;
            }
#endif          
        }
        firstDim = 1; /* skip checking the record dimension */
    }

    for (i=firstDim; i<(*varp)->ndims; i++) {
#ifdef RELAX_COORD_BOUND
        if (start[i] < 0 || start[i] > (*varp)->shape[i]) {
            DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
            goto fn_exit;
        }
        if (start[i] == ncp->numrecs) {
            if (api == API_VAR1) {
                /* for var1 APIs, count[0] is considered of 1 */
                DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
                goto fn_exit;
            }
            else if (count != NULL && count[i] > 0) {
                DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
                goto fn_exit;
            }
        }
#else
        if (start[i] < 0 || start[i] >= (*varp)->shape[i]) {
            DEBUG_ASSIGN_ERROR(err, NC_EINVALCOORDS)
            goto fn_exit;
        }
#endif
    }

    if (api <= API_VAR1) {
        /* var1/var APIs have no count argument */
        err = NC_NOERR;
        goto fn_exit;
    }

    /* Check NC_EEDGE
     * for API vara/vars/varm, count cannot be NULL, except for scalars */
    if (count == NULL) {
        DEBUG_ASSIGN_ERROR(err, NC_EEDGE)
        goto fn_exit;
    }
    firstDim = 0;
    if (IS_RECVAR(*varp)) {
        if (count[0] < 0) { /* no negative count[] */
            DEBUG_ASSIGN_ERROR(err, NC_ENEGATIVECNT)
            goto fn_exit;
        }
        /* for record variable, [0] is the NC_UNLIMITED dimension */
        if (rw_flag == READ_REQ) { /* read cannot go beyond current numrecs */
            if (stride == NULL) {  /* for vara APIs */
                if (start[0] + count[0] > ncp->numrecs) {
                    DEBUG_ASSIGN_ERROR(err, NC_EEDGE)
                    goto fn_exit;
                }
            }
            else { /* for vars/varm APIs */
                if (count[0] > 0 &&
                    start[0] + (count[0]-1) * stride[0] >= ncp->numrecs) {
                    DEBUG_ASSIGN_ERROR(err, NC_EEDGE)
                    goto fn_exit;
                }
            }
        }
        firstDim = 1; /* skip checking the record dimension */
    }

    for (i=firstDim; i<(*varp)->ndims; i++) {
        if ((*varp)->shape[i] < 0) {
            DEBUG_ASSIGN_ERROR(err, NC_EEDGE)
            goto fn_exit;
        }
        if (count[i] < 0) { /* no negative count[] */
            DEBUG_ASSIGN_ERROR(err, NC_ENEGATIVECNT)
            goto fn_exit;
        }

        if (stride == NULL) { /* for vara APIs */
            if (count[i] > (*varp)->shape[i] ||
                start[i] + count[i] > (*varp)->shape[i]) {
                DEBUG_ASSIGN_ERROR(err, NC_EEDGE)
                goto fn_exit;
            }
        }
        else { /* for vars APIs */
            if (count[i] > 0 &&
                start[i] + (count[i]-1) * stride[i] >= (*varp)->shape[i]) {
                DEBUG_ASSIGN_ERROR(err, NC_EEDGE)
                goto fn_exit;
            }
        }
    }

    if (api <= API_VARA) {
        /* vara APIs have no stride argument */
        err = NC_NOERR;
        goto fn_exit;
    }

    /* Check NC_ESTRIDE */
    for (i=0; i<(*varp)->ndims; i++) {
        if (stride != NULL && stride[i] == 0) {
            DEBUG_ASSIGN_ERROR(err, NC_ESTRIDE)
            goto fn_exit;
        }
    }

fn_exit:
    if (ncp->safe_mode == 1 && io_method == COLL_IO) {
        int min_st, mpireturn;
        TRACE_COMM(MPI_Allreduce)(&err, &min_st, 1, MPI_INT, MPI_MIN, ncp->comm);
        if (mpireturn != MPI_SUCCESS)
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        if (err == NC_NOERR) err = min_st;
    }
    return err;
}

/*----< ncmpio_set_pnetcdf_hints() >-----------------------------------------*/
/* this is where the I/O hints designated to pnetcdf are extracted */
void ncmpio_set_pnetcdf_hints(NC *ncp, MPI_Info info)
{
    char value[MPI_MAX_INFO_VAL];
    int  flag;

    if (info == MPI_INFO_NULL) return;

    /* nc_header_align_size, nc_var_align_size, and r_align * take effect when
     * a file is created or opened and later adding more header or variable
     * data */

    /* extract PnetCDF hints from user info object */
    MPI_Info_get(info, "nc_header_align_size", MPI_MAX_INFO_VAL-1, value,
                 &flag);
    if (flag) {
        errno = 0;  /* errno must set to zero before calling strtoll */
        ncp->h_align = strtoll(value,NULL,10);
        if (errno != 0) ncp->h_align = 0;
        else if (ncp->h_align < 0) ncp->h_align = 0;
    }

    MPI_Info_get(info, "nc_var_align_size", MPI_MAX_INFO_VAL-1, value, &flag);
    if (flag) {
        errno = 0;  /* errno must set to zero before calling strtoll */
        ncp->v_align = strtoll(value,NULL,10);
        if (errno != 0) ncp->v_align = 0;
        else if (ncp->v_align < 0) ncp->v_align = 0;
    }

    MPI_Info_get(info, "nc_record_align_size", MPI_MAX_INFO_VAL-1, value,
                 &flag);
    if (flag) {
        errno = 0;  /* errno must set to zero before calling strtoll */
        ncp->r_align = strtoll(value,NULL,10);
        if (errno != 0) ncp->r_align = 0;
        else if (ncp->r_align < 0) ncp->r_align = 0;
    }

    /* get header reading chunk size from info */
    MPI_Info_get(info, "nc_header_read_chunk_size", MPI_MAX_INFO_VAL-1, value,
                 &flag);
    if (flag) {
        errno = 0;  /* errno must set to zero before calling strtoll */
        ncp->chunk = strtoll(value,NULL,10);
        if (errno != 0) ncp->chunk = 0;
        else if (ncp->chunk < 0) ncp->chunk = 0;
    }
    
    /* Log related hint */
    ncp->loghints = NC_LOG_HINT_DEL_ON_CLOSE | NC_LOG_HINT_FLUSH_ON_READ | NC_LOG_HINT_FLUSH_ON_SYNC;
    MPI_Info_get(info, "pnetcdf_bb", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "enable") == 0){
        ncp->loghints |= NC_LOG_HINT_LOG_ENABLE;
    }
    MPI_Info_get(info, "pnetcdf_bb_del_on_close", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "disable") == 0){
        ncp->loghints ^= NC_LOG_HINT_DEL_ON_CLOSE;
    }
    MPI_Info_get(info, "pnetcdf_bb_flush_on_wait", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "enable") == 0){
        ncp->loghints |= NC_LOG_HINT_FLUSH_ON_WAIT;
    }
    MPI_Info_get(info, "pnetcdf_bb_flush_on_sync", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "disable") == 0){
        ncp->loghints ^= NC_LOG_HINT_FLUSH_ON_SYNC;
    }
    MPI_Info_get(info, "pnetcdf_bb_overwrite", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "enable") == 0){
        ncp->loghints |= NC_LOG_HINT_LOG_OVERWRITE;
    }
    MPI_Info_get(info, "pnetcdf_bb_check", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "enable") == 0){
        ncp->loghints |= NC_LOG_HINT_LOG_CHECK;
    }
    MPI_Info_get(info, "pnetcdf_bb_flush_on_read", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag && strcasecmp(value, "disable") == 0){
        ncp->loghints ^= NC_LOG_HINT_FLUSH_ON_READ;
    }
    MPI_Info_get(info, "pnetcdf_bb_flush_buffer_size", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag){
        long int bsize = strtol(value, NULL, 0);
        if (bsize < 0) {
            bsize = 0;
        }
        ncp->logflushbuffersize = (size_t)bsize; // Unit: byte 
    }
    else{
        ncp->logflushbuffersize = 0; // <= 0 means unlimited 
    }
    MPI_Info_get(info, "pnetcdf_bb_dirname", MPI_MAX_INFO_VAL - 1, value, &flag);
    if (flag) {
        strncpy(ncp->logbase, value, PATH_MAX);    
    }
    else {
        strncpy(ncp->logbase, ".", PATH_MAX);    
    }

#ifdef ENABLE_SUBFILING
    MPI_Info_get(info, "pnetcdf_subfiling", MPI_MAX_INFO_VAL-1, value, &flag);
    if (flag && strcasecmp(value, "disable") == 0)
        ncp->subfile_mode = 0;

    MPI_Info_get(info, "nc_num_subfiles", MPI_MAX_INFO_VAL-1, value, &flag);
    if (flag) {
        errno = 0;
        ncp->num_subfiles = strtoll(value,NULL,10);
        if (errno != 0) ncp->num_subfiles = 0;
        else if (ncp->num_subfiles < 0) ncp->num_subfiles = 0;
    }
    if (ncp->subfile_mode == 0) ncp->num_subfiles = 0;
#endif
}

/*----< ncmpii_check_mpifh() >-----------------------------------------------*/
int
ncmpii_check_mpifh(NC  *ncp,
                   int  collective)
{
    int mpireturn;

    if (collective && NC_indep(ncp)) /* collective handle but in indep mode */
        DEBUG_RETURN_ERROR(NC_EINDEP)

    if (!collective && !NC_indep(ncp)) /* indep handle but in collective mode */
        DEBUG_RETURN_ERROR(NC_ENOTINDEP)

    /* PnetCDF's default mode is collective. MPI file handle, collective_fh,
     * will never be MPI_FILE_NULL
     */

    if (!collective && ncp->independent_fh == MPI_FILE_NULL) {
        TRACE_IO(MPI_File_open)(MPI_COMM_SELF, (char*)ncp->path,
                                ncp->mpiomode, ncp->mpiinfo,
                                &ncp->independent_fh);
        if (mpireturn != MPI_SUCCESS)
            return ncmpii_handle_error(mpireturn, "MPI_File_open");
    }

    return NC_NOERR;
}

