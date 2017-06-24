/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>   /* open() */
#include <unistd.h>  /* read(), close() */
#include <assert.h>  /* assert() */
#include <errno.h>   /* errno */

#include <dispatch.h>
#include <pnetcdf.h>
#include <pnc_debug.h>

/* TODO: the following 3 global variables make PnetCDF not thread safe */

/* static variables are initialized to NULLs */
static PNC *pnc_filelist[NC_MAX_NFILES];
static int  pnc_numfiles;

/* This is the default create format for ncmpi_create and nc__create.
 * The use of this file scope variable is not thread-safe.
 */
static int ncmpi_default_create_format = NC_FORMAT_CLASSIC;

#define NCMPII_HANDLE_ERROR(func) \
    if (mpireturn != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(mpireturn, errorString, &errorStringLen); \
        printf("%s error at line %d file %s (%s)\n", func, __LINE__, __FILE__, errorString); \
    }

/*----< add_to_PNCList() >---------------------------------------------------*/
static int
add_to_PNCList(PNC *pncp,
               int *new_id)
{   
    int i;

    assert(pncp != NULL);

    *new_id = -1;
    for (i=0; i<NC_MAX_NFILES; i++) { /* find the first unused element */
        if (pnc_filelist[i] == NULL) {
            *new_id = i;
            break;
        }
    }
    if (*new_id == -1) /* Too many netcdf files open */
        DEBUG_RETURN_ERROR(NC_ENFILE)

    pnc_filelist[*new_id] = pncp;  /* store the pointer */
    pnc_numfiles++;                /* increment number of files opened */
    return NC_NOERR;
}

/*----< del_from_PNCList() >-------------------------------------------------*/
static void
del_from_PNCList(int ncid)
{
    /* validity of ncid should have been checked already */
    pnc_filelist[ncid] = NULL;
    pnc_numfiles--;
}

#if 0 /* refer to netCDF library's USE_REFCOUNT */
static PNC*
find_in_PNCList_by_name(const char* path)
{
    int i;
    PNC* pncp = NULL;
    for (i=0; i<NC_MAX_NFILES; i++) {
         if (pnc_filelist[i] == NULL) continue;
         if (strcmp(pnc_filelist[i]->path, path) == 0) {
             pncp = pnc_filelist[i];
             break;
         }
    }
    return pncp;
}
#endif

/*----< PNC_check_id() >-----------------------------------------------------*/
int
PNC_check_id(int ncid, PNC **pncp)
{
    assert(pncp != NULL);

    if (pnc_numfiles == 0 || ncid < 0 || ncid >= NC_MAX_NFILES)
        DEBUG_RETURN_ERROR(NC_EBADID)

    *pncp = pnc_filelist[ncid];

    return NC_NOERR;
}

/*----< ncmpi_create() >-----------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_create(MPI_Comm    comm,
             const char *path,
             int         cmode,
             MPI_Info    info,
             int        *ncidp)
{
    int default_format, rank, status=NC_NOERR, err;
    int safe_mode=0, mpireturn, root_cmode;
    char *env_str;
    PNC *pncp;
    PNC_Dispatch *dispatcher;

    MPI_Comm_rank(comm, &rank);

#ifdef PNETCDF_DEBUG
    safe_mode = 1;
    /* this configure time setting will be overwritten by the run-time
     * environment variable PNETCDF_SAFE_MODE */
#endif
    /* get environment variable PNETCDF_SAFE_MODE
     * if it is set to 1, then we perform a strict parameter consistent test
     */
    if ((env_str = getenv("PNETCDF_SAFE_MODE")) != NULL) {
        if (*env_str == '0') safe_mode = 0;
        else                 safe_mode = 1;
        /* if PNETCDF_SAFE_MODE is set but without a value, *env_str can
         * be '\0' (null character). In this case, safe_mode is enabled */
    }

    /* path's validity is checked in MPI-IO with error code MPI_ERR_BAD_FILE
     * path consistency is checked in MPI-IO with error code MPI_ERR_NOT_SAME
     */
    if (path == NULL || *path == '\0') DEBUG_RETURN_ERROR(NC_EBAD_FILE)

    /* Check cmode consistency */
    root_cmode = cmode; /* only root's matters */
    TRACE_COMM(MPI_Bcast)(&root_cmode, 1, MPI_INT, 0, comm);
    NCMPII_HANDLE_ERROR("MPI_Bcast")

    /* Overwrite cmode with root's cmode */
    if (root_cmode != cmode) {
        cmode = root_cmode;
        DEBUG_ASSIGN_ERROR(status, NC_EMULTIDEFINE_CMODE)
    }

    if (safe_mode) { /* sync status among all processes */
        err = status;
        TRACE_COMM(MPI_Allreduce)(&err, &status, 1, MPI_INT, MPI_MIN, comm);
        NCMPII_HANDLE_ERROR("MPI_Allreduce")
    }
    /* continue to use root's cmode to create the file, but will report cmode
     * inconsistency error, if there is any */

    /* TODO: Use cmode to tell the file format which is later used to select
     * the right dispatcher. For now, we have only one dispatcher, ncmpi.
     */
    dispatcher = ncmpii_inq_dispatcher();

#if 0 /* refer to netCDF library's USE_REFCOUNT */
    /* check whether this path is already opened */
    pncp = find_in_PNCList_by_name(path);
    if (pncp != NULL) return NC_ENFILE;
#endif

    /* Create a PNC object and save the dispatcher pointer */
    pncp = (PNC*) malloc(sizeof(PNC));
    if (pncp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
    pncp->path = (char*) malloc(strlen(path)+1);
    if (pncp->path == NULL) {
        free(pncp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(pncp->path, path);
    pncp->mode = cmode;
    pncp->dispatch = dispatcher;

    /* calling the create subroutine */
    err = dispatcher->create(comm, path, cmode, info, &pncp->ncp);
    if (status == NC_NOERR) status = err;

    if (status != NC_NOERR && status != NC_EMULTIDEFINE_CMODE) {
        free(pncp->path);
        free(pncp);
        return status;
    }

    /* set the file format version based on the create mode, cmode */
    ncmpi_inq_default_format(&default_format);
    if (cmode & NC_64BIT_DATA) {
        pncp->format = NC_FORMAT_CDF5;
    } else if (cmode & NC_64BIT_OFFSET) {
        pncp->format = NC_FORMAT_CDF2;
    } else {
        pncp->format = default_format;
    }

    /* add to the PNCList and obtain ncid */
    err = add_to_PNCList(pncp, ncidp);
    if (err != NC_NOERR) {
        free(pncp->path);
        free(pncp);
        return err;
    }
    return status;
}

/*----< ncmpi_open() >-------------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_open(MPI_Comm    comm,
           const char *path,
           int         omode,
           MPI_Info    info,
           int        *ncidp)
{
    int rank, format, msg[2], status=NC_NOERR, err;
    int safe_mode=0, mpireturn, root_omode;
    char *env_str;
    PNC *pncp;
    PNC_Dispatch *dispatcher;

    MPI_Comm_rank(comm, &rank);

#ifdef PNETCDF_DEBUG
    safe_mode = 1;
    /* this configure time setting will be overwritten by the run-time
     * environment variable PNETCDF_SAFE_MODE */
#endif
    /* get environment variable PNETCDF_SAFE_MODE
     * if it is set to 1, then we perform a strict parameter consistent test
     */
    if ((env_str = getenv("PNETCDF_SAFE_MODE")) != NULL) {
        if (*env_str == '0') safe_mode = 0;
        else                 safe_mode = 1;
        /* if PNETCDF_SAFE_MODE is set but without a value, *env_str can
         * be '\0' (null character). In this case, safe_mode is enabled */
    }

    /* path's validity is checked in MPI-IO with error code MPI_ERR_BAD_FILE
     * path consistency is checked in MPI-IO with error code MPI_ERR_NOT_SAME
     */
    if (path == NULL || *path == '\0') DEBUG_RETURN_ERROR(NC_EBAD_FILE)

    /* Check the file signature to tell the file format which is later used to
     * select the right dispatcher.
     */
    format = NC_FORMAT_UNKNOWN;
    if (rank == 0) {
        err = ncmpi_inq_file_format(path, &format);
        if (err != NC_NOERR) format = err;
        else if (format == NC_FORMAT_UNKNOWN) format = NC_ENOTNC;
    }
    msg[0] = format; /* only root's matters */

    /* Check omode consistency:
     * Note if omode contains NC_NOWRITE, it is equivalent to NC_CLOBBER.
     * In pnetcdf.h, they both are defined the same value, 0.
     * Only root's omode matters.
     */
    msg[1] = omode; /* only root's matters */

    TRACE_COMM(MPI_Bcast)(&msg, 2, MPI_INT, 0, comm);
    NCMPII_HANDLE_ERROR("MPI_Bcast")

    /* check format error (a fatal error, must return now) */
    format = msg[0];
    if (format < 0) return format; /* all netCDF errors are negative */

    /* check omode consistency */
    root_omode = msg[1];
    if (root_omode != omode) {
        omode = root_omode;
        DEBUG_ASSIGN_ERROR(status, NC_EMULTIDEFINE_OMODE)
    }

    if (safe_mode) { /* sync status among all processes */
        err = status;
        TRACE_COMM(MPI_Allreduce)(&err, &status, 1, MPI_INT, MPI_MIN, comm);
        NCMPII_HANDLE_ERROR("MPI_Allreduce")
    }
    /* continue to use root's omode to open the file, but will report omode
     * inconsistency error, if there is any */

    if (format == NC_FORMAT_CLASSIC ||
        format == NC_FORMAT_CDF2 ||
        format == NC_FORMAT_CDF5) {
        /* TODO: currently we only have ncmpi dispatcher. Need to add other
         * dispatchers once they are available
         */
        dispatcher = ncmpii_inq_dispatcher();
    }
    else if (format == NC_FORMAT_NETCDF4_CLASSIC) {
        fprintf(stderr,"NC_FORMAT_NETCDF4_CLASSIC is not yet supported\n");
        DEBUG_RETURN_ERROR(NC_ENOTSUPPORT)
    }
    else if (format == NC_FORMAT_NETCDF4) {
        fprintf(stderr,"NC_FORMAT_NETCDF4 is not yet supported\n");
        DEBUG_RETURN_ERROR(NC_ENOTSUPPORT)
    }
    else { /* unrecognized file format */
        DEBUG_RETURN_ERROR(NC_ENOTNC)
    }

    /* Create a PNC object and save its dispatcher pointer */
    pncp = (PNC*) malloc(sizeof(PNC));
    if (pncp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
    pncp->path = (char*) malloc(strlen(path)+1);
    if (pncp->path == NULL) {
        free(pncp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }
    strcpy(pncp->path, path);
    pncp->mode = omode;
    pncp->dispatch = dispatcher;

    /* calling the open subroutine */
    err = dispatcher->open(comm, path, omode, info, &pncp->ncp);
    if (status == NC_NOERR) status = err;

    if (status != NC_NOERR && status != NC_EMULTIDEFINE_OMODE) {
        free(pncp->path);
        free(pncp);
        return status;
    }

    pncp->format = format;

    /* add to the PNCList and obtain ncid */
    err = add_to_PNCList(pncp, ncidp);
    if (err != NC_NOERR) {
        free(pncp->path);
        free(pncp);
        return err;
    }
    return status;
}

/*----< ncmpi_close() >------------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_close(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_close() */
    err = pncp->dispatch->close(pncp->ncp);

    /* Remove from the PNCList, even if err != NC_NOERR */
    del_from_PNCList(ncid);

    /* free the PNC object */
    free(pncp->path);
    free(pncp);

    return err;
}

/*----< ncmpi_enddef() >-----------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_enddef(int ncid) {
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_enddef() */
    return pncp->dispatch->enddef(pncp->ncp);
}

/*----< ncmpi__enddef() >----------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi__enddef(int        ncid,
              MPI_Offset h_minfree,
              MPI_Offset v_align,
              MPI_Offset v_minfree,
              MPI_Offset r_align)
{   
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi__enddef() */
    return pncp->dispatch->_enddef(pncp->ncp, h_minfree, v_align,
                                              v_minfree, r_align);
}

/*----< ncmpi_redef() >------------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_redef(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_redef() */
    return pncp->dispatch->redef(pncp->ncp);
}

/*----< ncmpi_sync() >-------------------------------------------------------*/
/* This API is a collective subroutine, and must be called in data mode, no
 * matter if it is in collective or independent data mode.
 */
int
ncmpi_sync(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_sync() */
    return pncp->dispatch->sync(pncp->ncp);
}

/*----< ncmpi_abort() >------------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_abort(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_abort() */
    err = pncp->dispatch->abort(pncp->ncp);

    /* Remove from the PNCList, even if err != NC_NOERR */
    del_from_PNCList(ncid);

    /* free the PNC object */
    free(pncp->path);
    free(pncp);

    return err;
}

/*----< ncmpi_set_fill() >---------------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_set_fill(int  ncid,
               int  fill_mode,
               int *old_fill_mode)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_set_fill() */
    return pncp->dispatch->set_fill(pncp->ncp, fill_mode, old_fill_mode);
}

/*----< ncmpi_inq_format() >-------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_format(int  ncid,
                 int *formatp)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    *formatp = pncp->format;

    return NC_NOERR;
}

/*----< ncmpi_inq_file_format() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_file_format(const char *filename,
                      int        *formatp) /* out */
{
    char *cdf_signature="CDF";
    char *hdf5_signature="\211HDF\r\n\032\n";
    char signature[8];
    int fd;
    ssize_t rlen;

    *formatp = NC_FORMAT_UNKNOWN;

    /* must include config.h on 32-bit machines, as AC_SYS_LARGEFILE is called
     * at the configure time and it defines _FILE_OFFSET_BITS to 64 if large
     * file feature is supported.
     */
    if ((fd = open(filename, O_RDONLY, 00400)) == -1) {
             if (errno == ENOENT)       DEBUG_RETURN_ERROR(NC_ENOENT)
        else if (errno == EACCES)       DEBUG_RETURN_ERROR(NC_EACCESS)
        else if (errno == ENAMETOOLONG) DEBUG_RETURN_ERROR(NC_EBAD_FILE)
        else {
            fprintf(stderr,"Error on opening file %s (%s)\n",filename,strerror(errno));
            DEBUG_RETURN_ERROR(NC_EFILE)
        }
    }
    /* get first 8 bytes of file */
    rlen = read(fd, signature, 8);
    if (rlen != 8) {
        close(fd); /* ignore error */
        DEBUG_RETURN_ERROR(NC_EFILE)
    }
    if (close(fd) == -1) {
        DEBUG_RETURN_ERROR(NC_EFILE)
    }

    if (memcmp(signature, hdf5_signature, 8) == 0) {
        /* TODO: whether the file is NC_FORMAT_NETCDF4_CLASSIC is determined by
         * HDF5 attribute "_nc3_strict" which requires a call to H5Aget_name().
         * For now, we do not distinguish NC_CLASSIC_MODEL, but simply return
         * NETCDF4 format.
         */
        *formatp = NC_FORMAT_NETCDF4;
    }
    else if (memcmp(signature, cdf_signature, 3) == 0) {
             if (signature[3] == 5)  *formatp = NC_FORMAT_CDF5;
        else if (signature[3] == 2)  *formatp = NC_FORMAT_CDF2;
        else if (signature[3] == 1)  *formatp = NC_FORMAT_CLASSIC;
    }

    return NC_NOERR;
}

/*----< ncmpi_inq_version() >-----------------------------------------------*/
int
ncmpi_inq_version(int ncid, int *nc_mode)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    if (pncp->format == 5) {
        *nc_mode = NC_64BIT_DATA;
    } else if (pncp->format == 2) {
        *nc_mode = NC_64BIT_OFFSET;
    } else if (pncp->format == 1) {
        *nc_mode = NC_CLASSIC_MODEL;
    }

    return NC_NOERR;
}

/*----< ncmpi_inq() >------------------------------------------------------*/
int
ncmpi_inq(int  ncid,
          int *ndimsp,
          int *nvarsp,
          int *nattsp,
          int *xtendimp)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_abort() */
    return pncp->dispatch->inq(pncp->ncp, ndimsp, nvarsp, nattsp, xtendimp);
}

/*----< ncmpi_inq_ndims() >--------------------------------------------------*/
int
ncmpi_inq_ndims(int  ncid,
                int *ndimsp)
{
    return ncmpi_inq(ncid, ndimsp, NULL, NULL, NULL);
}

/*----< ncmpi_inq_nvars() >--------------------------------------------------*/
int
ncmpi_inq_nvars(int  ncid,
                int *nvarsp)
{
    return ncmpi_inq(ncid, NULL, nvarsp, NULL, NULL);
}

/*----< ncmpi_inq_natts() >--------------------------------------------------*/
int
ncmpi_inq_natts(int  ncid,
                int *nattsp)
{
    return ncmpi_inq(ncid, NULL, NULL, nattsp, NULL);
}

/*----< ncmpi_inq_unlimdim() >-----------------------------------------------*/
int
ncmpi_inq_unlimdim(int  ncid,
                   int *unlimdimidp)
{
    return ncmpi_inq(ncid, NULL, NULL, NULL, unlimdimidp);
}

/*----< ncmpi_inq_path() >---------------------------------------------------*/
/* Get the file pathname which was used to open/create the ncid's file.
 * This is an independent subroutine.
 */
int
ncmpi_inq_path(int   ncid,
               int  *pathlen,/* Ignored if NULL */
               char *path)   /*  must already be allocated. Ignored if NULL */
{        
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_path() */
    return pncp->dispatch->inq_misc(pncp->ncp, pathlen, path, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_num_fix_vars() >-------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_num_fix_vars(int ncid, int *num_fix_varsp)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_num_fix_vars() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, num_fix_varsp, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_num_rec_vars() >-------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_num_rec_vars(int ncid, int *num_rec_varsp)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_num_rec_vars() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, num_rec_varsp,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_striping() >-----------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_striping(int ncid, int *striping_size, int *striping_count)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_striping() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    striping_size, striping_count, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_header_size() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_header_size(int ncid, MPI_Offset *header_size)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_header_size() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, header_size, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_header_extent() >------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_header_extent(int ncid, MPI_Offset *header_extent)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_header_extent() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, header_extent, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_recsize() >-----------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_recsize(int ncid, MPI_Offset *recsize)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_recsize() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, recsize, NULL,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_put_size() >----------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_put_size(int ncid, MPI_Offset *put_size)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_put_size() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, put_size,
                                    NULL, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_get_size() >----------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_get_size(int ncid, MPI_Offset *get_size)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_get_size() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    get_size, NULL, NULL, NULL, NULL);
}

/*----< ncmpi_inq_file_info() >---------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_file_info(int ncid, MPI_Info *info)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_end_indep_data() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, info, NULL, NULL, NULL);
}

/* ncmpi_get_file_info() is now deprecated, replaced by ncmpi_inq_file_info() */
int
ncmpi_get_file_info(int ncid, MPI_Info *info)
{
    return ncmpi_inq_file_info(ncid, info);
}

/*----< ncmpi_begin_indep_data() >-------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_begin_indep_data(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_begin_indep_data() */
    return pncp->dispatch->begin_indep_data(pncp->ncp);
}

/*----< ncmpi_end_indep_data() >---------------------------------------------*/
/* This is a collective subroutine. */
int
ncmpi_end_indep_data(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_end_indep_data() */
    return pncp->dispatch->end_indep_data(pncp->ncp);
}

/*----< ncmpi_sync_numrecs() >-----------------------------------------------*/
/* this API is collective, but can be called in independent data mode.
 * Note numrecs (number of records) is always sync-ed in memory and file in
 * collective data mode.
 */
int
ncmpi_sync_numrecs(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_sync_numrecs() */
    return pncp->dispatch->sync_numrecs(pncp->ncp);
}

/*----< ncmpi_set_default_format() >-----------------------------------------*/
/* This function sets a default create file format.
 * Valid formats are NC_FORMAT_CLASSIC, NC_FORMAT_CDF2, and NC_FORMAT_CDF5
 * This API is NOT collective, as there is no way to check against an MPI
 * communicator. It should be called by all MPI processes that intend to
 * create a file later. Consistency check will have to be done in other APIs.
 */
int
ncmpi_set_default_format(int format, int *old_formatp)
{
    /* Return existing format if desired. */
    if (old_formatp != NULL)
        *old_formatp = ncmpi_default_create_format;

    /* Make sure only valid format is set. */
    if (format != NC_FORMAT_CLASSIC &&
        format != NC_FORMAT_CDF2 &&
        format != NC_FORMAT_CDF5) {
        DEBUG_RETURN_ERROR(NC_EINVAL)
    }
    ncmpi_default_create_format = format;

    return NC_NOERR;
}

/*----< ncmpi_inq_default_format() >-----------------------------------------*/
/* returns a value suitable for a create flag.  Will return one or more of the
 * following values OR-ed together:
 * NC_64BIT_OFFSET, NC_CLOBBER, NC_LOCK, NC_SHARE */
int
ncmpi_inq_default_format(int *formatp)
{
    if (formatp == NULL) DEBUG_RETURN_ERROR(NC_EINVAL)

    *formatp = ncmpi_default_create_format;
    return NC_NOERR;
}

/*----< ncmpi_inq_files_opened() >-------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_files_opened(int *num,    /* cannot be NULL */
                       int *ncids)  /* can be NULL */
{
    int i;

    if (num == NULL) DEBUG_RETURN_ERROR(NC_EINVAL)

    *num = pnc_numfiles;

    if (ncids != NULL) { /* ncids can be NULL */
        *num = 0;
        for (i=0; i<NC_MAX_NFILES; i++) {
            if (pnc_filelist[i] != NULL) {
                    ncids[*num] = i;
                (*num)++;
            }
        }
        assert(*num == pnc_numfiles);
    }
    return NC_NOERR;
}

/*----< ncmpi_inq_nreqs() >--------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_nreqs(int  ncid,
                int *nreqs) /* number of pending nonblocking requests */
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    if (nreqs == NULL) DEBUG_RETURN_ERROR(NC_EINVAL)

    /* calling the subroutine that implements ncmpi_inq_path() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, nreqs, NULL, NULL);
}

/*----< ncmpi_inq_buffer_usage() >-------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_buffer_usage(int         ncid,
                       MPI_Offset *usage) /* amount of space used so far */
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    if (usage == NULL) DEBUG_RETURN_ERROR(NC_EINVAL)

    /* calling the subroutine that implements ncmpi_inq_path() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, usage, NULL);
}

/*----< ncmpi_inq_buffer_size() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_buffer_size(int         ncid,
                      MPI_Offset *buf_size) /* amount of space attached */
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    if (buf_size == NULL) DEBUG_RETURN_ERROR(NC_EINVAL)

    /* calling the subroutine that implements ncmpi_inq_path() */
    return pncp->dispatch->inq_misc(pncp->ncp, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL, buf_size);
}

/*----< ncmpi_buffer_attach() >-----------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_buffer_attach(int        ncid,
                    MPI_Offset bufsize) /* amount of memory space allowed for
                                           PnetCDF library to buffer the
                                           nonblocking requests */
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_buffer_attach() */
    return pncp->dispatch->buffer_attach(pncp->ncp, bufsize);
}

/*----< ncmpi_buffer_detach() >-----------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_buffer_detach(int ncid)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_buffer_detach() */
    return pncp->dispatch->buffer_detach(pncp->ncp);
}
