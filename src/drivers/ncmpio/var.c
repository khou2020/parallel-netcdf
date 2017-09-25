/*
 *  Copyright (C) 2003, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#include <stdio.h>
#include <string.h> /* memset() */
#include <assert.h>

#include <mpi.h>

#include <pnc_debug.h>
#include <common.h>
#include "nc.h"
#include "ncx.h"
#include "rnd.h"
#include "utf8proc.h"

/*----< ncmpii_free_NC_var() >------------------------------------------------*/
/*
 * Free var
 * Formerly
NC_free_var(var)
 */
inline void
ncmpii_free_NC_var(NC_var *varp)
{
    if (varp == NULL) return;
    ncmpii_free_NC_attrarray(&varp->attrs);
    ncmpii_free_NC_string(varp->name);
#ifdef ENABLE_SUBFILING
    if (varp->num_subfiles > 1) /* deallocate it */
        NCI_Free(varp->dimids_org);
#endif
    if (varp->shape  != NULL) NCI_Free(varp->shape);
    if (varp->dsizes != NULL) NCI_Free(varp->dsizes);
    if (varp->dimids != NULL) NCI_Free(varp->dimids);

    NCI_Free(varp);
}


/*----< ncmpii_new_x_NC_var() >-----------------------------------------------*/
/*
 * Used by ncmpii_new_NC_var() and ncx_get_NC_var()
 */
NC_var *
ncmpii_new_x_NC_var(NC_string *strp,
                    int        ndims)
{
    NC_var *varp;

    varp = (NC_var *) NCI_Calloc(1, sizeof(NC_var));
    if (varp == NULL) return NULL;

    varp->name = strp;
    varp->ndims = ndims;

    if (ndims > 0) {
        varp->shape  = (MPI_Offset*)NCI_Calloc(ndims, SIZEOF_MPI_OFFSET);
        varp->dsizes = (MPI_Offset*)NCI_Calloc(ndims, SIZEOF_MPI_OFFSET);
        varp->dimids = (int *)      NCI_Calloc(ndims, SIZEOF_INT);
    }

    varp->xsz = 0;
    varp->len = 0;
    varp->begin = 0;

    return varp;
}

/*----< ncmpii_new_NC_var() >------------------------------------------------*/
/*
 * Formerly, NC_new_var()
 */
static int
ncmpii_new_NC_var(NC_vararray  *vcap,
                  const char   *name,  /* normalized variable name (NULL terminated) */
                  nc_type       type,
                  int           ndims,
                  const int    *dimids,
                  NC_var      **varp)
{
    NC_string *strp;

    strp = ncmpii_new_NC_string(strlen(name), name);
    if (strp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)

    *varp = ncmpii_new_x_NC_var(strp, ndims);
    if (*varp == NULL ) {
        ncmpii_free_NC_string(strp);
        DEBUG_RETURN_ERROR(NC_ENOMEM)
    }

    (*varp)->type = type;

    if (ndims != 0 && dimids != NULL)
        memcpy((*varp)->dimids, dimids, (size_t)ndims * SIZEOF_INT);

#ifndef SEARCH_NAME_LINEARLY
    if (vcap != NULL) { /* insert new var to hash table */
        int key;
        NC_nametable *nameT = vcap->nameT; /* var name lookup table */

        /* hash the var name into a key for name lookup */
        key = HASH_FUNC(name);

        /* allocate or expand the space for nameT[key].list */
        if (nameT[key].num % NC_NAME_TABLE_CHUNK == 0)
            nameT[key].list = (int*) NCI_Realloc(nameT[key].list,
                              (size_t)(nameT[key].num+NC_NAME_TABLE_CHUNK) * SIZEOF_INT);

        /* add the new variable ID to the name lookup table
         * the new varid will be vcap->ndefined
         */
        nameT[key].list[nameT[key].num] = vcap->ndefined;
        nameT[key].num++;
    }
    /* else case is for variable duplication called from dup_NC_var() */
#endif

    return NC_NOERR;
}

/*----< ncmpii_update_name_lookup_table() >----------------------------------*/
/* remove the entry in lookup table for oldname and add a new entry for
 * newname
 */
int
ncmpii_update_name_lookup_table(NC_nametable *nameT,
                                const int     id,
                                const char   *oldname,  /*    normalized */
                                const char   *unewname) /* un-normalized */
{
    int i, key;
    char *name; /* normalized name string */

    /* remove the old name from the lookup table
     * hash the var name into a key for name lookup
     */
    key = HASH_FUNC(oldname);
    for (i=0; i<nameT[key].num; i++) {
        if (nameT[key].list[i] == id) break;
    }
    assert(i!=nameT[key].num);

    /* coalesce the id array */
    for (; i<nameT[key].num-1; i++)
        nameT[key].list[i] = nameT[key].list[i+1]; 

    /* decrease the number of IDs and free space if necessary */
    nameT[key].num--;
    if (nameT[key].num == 0) {
        NCI_Free(nameT[key].list);
        nameT[key].list = NULL;
    }

    /* normalized version of uname */
    name = (char *)ncmpii_utf8proc_NFC((const unsigned char *)unewname);
    if (name == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)

    /* hash the var name into a key for name lookup */
    key = HASH_FUNC(name);
    free(name);

    /* add the new name to the lookup table
     * Note unewname must have already been checked for existence
     */
    if (nameT[key].num % NC_NAME_TABLE_CHUNK == 0)
        nameT[key].list = (int*) NCI_Realloc(nameT[key].list,
                          (size_t)(nameT[key].num+NC_NAME_TABLE_CHUNK) * SIZEOF_INT);
    nameT[key].list[nameT[key].num] = id;
    nameT[key].num++;

    return NC_NOERR;
}


/*----< dup_NC_var() >--------------------------------------------------------*/
NC_var *
dup_NC_var(const NC_var *rvarp)
{
    int err;
    NC_var *varp;

    /* note that name in rvarp->name->cp is already normalized */
    err = ncmpii_new_NC_var(NULL, rvarp->name->cp, rvarp->type, rvarp->ndims,
                            rvarp->dimids, &varp);
    if (err != NC_NOERR) return NULL;

    if (ncmpii_dup_NC_attrarray(&varp->attrs, &rvarp->attrs) != NC_NOERR) {
        ncmpii_free_NC_var(varp);
        return NULL;
    }

    /* copy the contents of shape may not be necessary, as one must call
     * ncmpii_NC_computeshapes() to recompute it after a new variable is
     * created */
    memcpy(varp->shape,  rvarp->shape,  (size_t)rvarp->ndims * SIZEOF_MPI_OFFSET);
    memcpy(varp->dsizes, rvarp->dsizes, (size_t)rvarp->ndims * SIZEOF_MPI_OFFSET);
    varp->xsz = rvarp->xsz;
    varp->len = rvarp->len;
    varp->begin = rvarp->begin;

    return varp;
}

/* vararray */

/*----< ncmpii_free_NC_vararray() >-------------------------------------------*/
/*
 * Free NC_vararray values.
 * formerly
NC_free_array()
 */
inline void
ncmpii_free_NC_vararray(NC_vararray *ncap)
{
    int i;

    assert(ncap != NULL);
    if (ncap->nalloc == 0) return;

    assert(ncap->value != NULL);
    for (i=0; i<ncap->ndefined; i++) {
        if (ncap->value[i] != NULL)
            ncmpii_free_NC_var(ncap->value[i]);
    }

    NCI_Free(ncap->value);
    ncap->value    = NULL;
    ncap->nalloc   = 0;
    ncap->ndefined = 0;

    /* free space allocated for var name lookup table */
    for (i=0; i<HASH_TABLE_SIZE; i++) {
        if (ncap->nameT[i].num > 0)
            NCI_Free(ncap->nameT[i].list);
        ncap->nameT[i].num = 0;
    }
}


/*----< ncmpii_dup_NC_vararray() >--------------------------------------------*/
int
ncmpii_dup_NC_vararray(NC_vararray       *ncap,
                       const NC_vararray *ref)
{
    int i, status=NC_NOERR;

    assert(ref != NULL);
    assert(ncap != NULL);

    if (ref->nalloc == 0) {
        ncap->nalloc = 0;
        ncap->ndefined = 0;
        ncap->value = NULL;
        return NC_NOERR;
    }

    if (ref->nalloc > 0) {
        ncap->value = (NC_var **) NCI_Calloc((size_t)ref->nalloc, sizeof(NC_var*));
        if (ncap->value == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)
        ncap->nalloc = ref->nalloc;
    }

    ncap->ndefined = 0;
    for (i=0; i<ref->ndefined; i++) {
        ncap->value[i] = dup_NC_var(ref->value[i]);
        if (ncap->value[i] == NULL) {
            DEBUG_ASSIGN_ERROR(status, NC_ENOMEM)
            break;
        }
    }

    if (status != NC_NOERR) {
        ncmpii_free_NC_vararray(ncap);
        return status;
    }

    ncap->ndefined = ref->ndefined;

    /* duplicate var name lookup table */
    for (i=0; i<HASH_TABLE_SIZE; i++) {
        ncap->nameT[i].num = ref->nameT[i].num;
        ncap->nameT[i].list = NULL;
        if (ncap->nameT[i].num > 0) {
            ncap->nameT[i].list = NCI_Malloc((size_t)ncap->nameT[i].num * SIZEOF_INT);
            memcpy(ncap->nameT[i].list, ref->nameT[i].list,
                   (size_t)ncap->nameT[i].num * SIZEOF_INT);
        }
    }

    return NC_NOERR;
}


/*
 * Add a new handle on the end of an array of handles
 * Formerly
NC_incr_array(array, tail)
 */
int
incr_NC_vararray(NC_vararray *ncap,
                 NC_var      *newvarp)
{
    NC_var **vp;

    assert(ncap != NULL);
    assert(newvarp != NULL);

    if (ncap->nalloc == 0) { /* no variable has been allocated yet */
        assert(ncap->ndefined == 0);
        vp = (NC_var **) NCI_Malloc(NC_ARRAY_GROWBY * sizeof(NC_var *));
        if (vp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)

        ncap->value = vp;
        ncap->nalloc = NC_ARRAY_GROWBY;
    }
    else if (ncap->ndefined + 1 > ncap->nalloc) {
        vp = (NC_var **) NCI_Realloc(ncap->value,
                         (size_t)(ncap->nalloc + NC_ARRAY_GROWBY) * sizeof(NC_var *));
        if (vp == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)

        ncap->value = vp;
        ncap->nalloc += NC_ARRAY_GROWBY;
    }

    if (newvarp != NULL) {
        ncap->value[ncap->ndefined] = newvarp;
        ncap->ndefined++;
    }

    return NC_NOERR;
}


inline static NC_var *
elem_NC_vararray(const NC_vararray *ncap,
                 int                varid)
{
    assert(ncap != NULL);
    /* cast needed for braindead systems with signed MPI_Offset */
    if ((varid < 0) ||  ncap->ndefined == 0 || varid >= ncap->ndefined)
        return NULL;

    assert(ncap->value != NULL);

    return ncap->value[varid];
}


/* End vararray per se */


#ifdef SEARCH_NAME_LINEARLY
/*
 * Step thru NC_VARIABLE array, seeking match on name.
 * If found, set the variable ID pointed by vardip, otherwise return NC_ENOTVAR
 * Formerly (sort of)
NC_hvarid
 */
static int
ncmpii_NC_findvar(const NC_vararray *ncap,
                  const char        *name,  /* normalized name */
                  int               *varidp)
{
    int varid;
    size_t nchars;

    assert (ncap != NULL);

    if (ncap->ndefined == 0) return NC_ENOTVAR;

    nchars = strlen(name);

    for (varid=0; varid<ncap->ndefined; varid++) {
        if (ncap->value[varid]->name->nchars == nchars &&
            strncmp(ncap->value[varid]->name->cp, name, nchars) == 0) {
            if (varidp != NULL) *varidp = varid;
            return NC_NOERR; /* found it */
        }
    }

    return NC_ENOTVAR; /* not found */
}
#else
/*----< ncmpii_NC_findvar() >------------------------------------------------*/
/* Check if the name has been used.
 * If yes, set the variable ID pointed by vardip, otherwise return NC_ENOTVAR
 */
static int
ncmpii_NC_findvar(const NC_vararray  *ncap,
                  const char         *name,  /* normalized name */
                  int                *varidp)
{
    int i, key, varid;

    assert (ncap != NULL);

    if (ncap->ndefined == 0) return NC_ENOTVAR;

    /* hash the var name into a key for name lookup */
    key = HASH_FUNC(name);

    /* check the list using linear search */
    for (i=0; i<ncap->nameT[key].num; i++) {
        varid = ncap->nameT[key].list[i];
        if (strcmp(name, ncap->value[varid]->name->cp) == 0) {
            if (varidp != NULL) *varidp = varid;
            return NC_NOERR; /* the name already exists */
        }
    }

    return NC_ENOTVAR; /* the name has never been used */
}
#endif

/*
 * For a netcdf type
 *  return the size of one element in the external representation.
 * Note that arrays get rounded up to X_ALIGN boundaries.
 * Formerly
NC_xtypelen
 * See also ncx_len()
 */
static int
ncx_szof(nc_type type)
{
    switch(type){
        case NC_BYTE:
        case NC_CHAR:   return (1);
        case NC_SHORT:  return (2);
        case NC_INT:    return X_SIZEOF_INT;
        case NC_FLOAT:  return X_SIZEOF_FLOAT;
        case NC_DOUBLE: return X_SIZEOF_DOUBLE;
        case NC_UBYTE:  return (1);
        case NC_USHORT: return (2);
        case NC_UINT:   return X_SIZEOF_UINT;
        case NC_INT64:  return X_SIZEOF_INT64;
        case NC_UINT64: return X_SIZEOF_UINT64;
        default:
             fprintf(stderr,"ncx_szof invalid type %d\n", type);
             assert(0);
    }
    /* default */
    return 0;
}

/*----< ncmpii_NC_var_shape64() >--------------------------------------------*/
/*
 * set varp->xsz, varp->shape and varp->len of a variable
 */
int
ncmpii_NC_var_shape64(NC_var            *varp,
                      const NC_dimarray *dims)
{
    int i;
    MPI_Offset product = 1;

    /* set the size of 1 element */
    varp->xsz = ncx_szof(varp->type);

    if (varp->ndims == 0) goto out;

    /*
     * use the user supplied dimension indices to determine the shape
     */
    for (i=0; i<varp->ndims; i++) {
        const NC_dim *dimp;

        if (varp->dimids[i] < 0)
            DEBUG_RETURN_ERROR(NC_EBADDIM)

        if (varp->dimids[i] >= ((dims != NULL) ? dims->ndefined : 1))
            DEBUG_RETURN_ERROR(NC_EBADDIM)

        /* get the pointer to the dim object */
        dimp = ncmpii_elem_NC_dimarray(dims, varp->dimids[i]);
        varp->shape[i] = dimp->size;

        /* check for record variable, only the highest dimension can
         * be unlimited */
        if (varp->shape[i] == NC_UNLIMITED && i != 0)
            DEBUG_RETURN_ERROR(NC_EUNLIMPOS)
    }

    /*
     * compute the dsizes, the right to left product of shape
     */
    product = 1;
    if (varp->ndims == 1) {
        if (varp->shape[0] == NC_UNLIMITED)
            varp->dsizes[0] = 1;
        else {
            varp->dsizes[0] = varp->shape[0];
            product = varp->shape[0];
        }
    }
    else { /* varp->ndims > 1 */
        varp->dsizes[varp->ndims-1] = varp->shape[varp->ndims-1];
        product = varp->shape[varp->ndims-1];
        for (i=varp->ndims-2; i>=0; i--) {
            if (varp->shape[i] != NC_UNLIMITED)
                product *= varp->shape[i];
            varp->dsizes[i] = product;
        }
    }

out :
    /*
     * For CDF-1 and CDF-2 formats, the total number of array elements
     * cannot exceed 2^32, unless this variable is the last fixed-size
     * variable, there is no record variable, and the file starting
     * offset of this variable is less than 2GiB.
     * We will check this in ncmpi_enddef() which calls ncmpii_NC_enddef()
     * which calls ncmpii_NC_check_vlens()
     *
    if (ncp->format < 5 && product >= X_UINT_MAX)
        DEBUG_RETURN_ERROR(NC_EVARSIZE)
     */

    /*
     * align variable size to 4 byte boundary, required by all netcdf
     * file formats
     */
    varp->len = product * varp->xsz;
    if (varp->len % 4 > 0)
        varp->len += 4 - varp->len % 4; /* round up */

    return NC_NOERR;
}

/*
 * Check whether variable size is less than or equal to vlen_max,
 * without overflowing in arithmetic calculations.  If OK, return 1,
 * else, return 0.  For CDF1 format or for CDF2 format on non-LFS
 * platforms, vlen_max should be 2^31 - 4, but for CDF2 format on
 * systems with LFS it should be 2^32 - 4.
 */
inline int
ncmpii_NC_check_vlen(NC_var     *varp,
                     MPI_Offset  vlen_max)
{
    int ii;
    MPI_Offset prod=varp->xsz;     /* product of xsz and dimensions so far */

    for (ii = IS_RECVAR(varp) ? 1 : 0; ii < varp->ndims; ii++) {
        if (varp->shape[ii] > vlen_max / prod) {
            return 0;           /* size in bytes won't fit in a 32-bit int */
        }
        prod *= varp->shape[ii];
    }
    return 1;                  /* OK */
}

/*----< ncmpii_NC_lookupvar() >----------------------------------------------*/
/*
 * Given valid ncp and varid, return var
 *  else NULL on error
 * Formerly
NC_hlookupvar()
 */
int
ncmpii_NC_lookupvar(NC      *ncp,
                    int      varid,
                    NC_var **varp)
{
    if (varid == NC_GLOBAL) /* Global is error in this context */
        DEBUG_RETURN_ERROR(NC_EGLOBAL)

    *varp = elem_NC_vararray(&ncp->vars, varid);
    if (*varp == NULL) /* could not find variable with varid */
        DEBUG_RETURN_ERROR(NC_ENOTVAR)

    return NC_NOERR;
}


/*----< ncmpii_def_var() >----------------------------------------------------*/
int
ncmpii_def_var(void       *ncdp,
               const char *name,
               nc_type     type,
               int         ndims,
               const int  *dimids,
               int        *varidp)
{
    int err=NC_NOERR;
    char *nname=NULL; /* normalized name */
    NC *ncp=(NC*)ncdp;
    NC_var *varp=NULL;

    /* check if called in define mode */
    if (!NC_indef(ncp)) {
        DEBUG_ASSIGN_ERROR(err, NC_ENOTINDEFINE)
        goto err_check;
    }

    if (name == NULL || *name == 0) {
        DEBUG_ASSIGN_ERROR(err, NC_EBADNAME)
        goto err_check;
    }

    if (strlen(name) > NC_MAX_NAME) {
        DEBUG_ASSIGN_ERROR(err, NC_EMAXNAME)
        goto err_check;
    }

    /* check if the name string is legal for netcdf format */
    err = ncmpii_NC_check_name(name, ncp->format);
    if (err != NC_NOERR) {
        DEBUG_TRACE_ERROR
        goto err_check;
    }

    /* check if type is a valid netcdf type */
    err = ncmpii_cktype(ncp->format, type);
    if (err != NC_NOERR) {
        DEBUG_TRACE_ERROR
        goto err_check;
    }

    /* TODO: make ndims of type MPI_Offset so ndims can be > 2^31-1 in CDF-5
     *
     * When checking against max number of dimensions per variable,
     * because there is no error code for this, thus we use NC_EINVAL
    */
    if (ndims < 0 || ndims == NC_MAX_VAR_DIMS) {
        DEBUG_ASSIGN_ERROR(err, NC_EINVAL)
        goto err_check;
    }

    /* Note we no longer limit the number of variables, as CDF file formats
     * impose no such limit. Thus, the value of NC_MAX_VARS has been changed
     * to NC_MAX_INT, as NC_vararray.ndefined is of type signd int and so is
     * nvars argument in ncmpi_inq_nvars()
     */
    if (ncp->vars.ndefined == NC_MAX_VARS) {
        DEBUG_ASSIGN_ERROR(err, NC_EMAXVARS)
        goto err_check;
    }

    /* create a normalized character string */
    nname = (char *)ncmpii_utf8proc_NFC((const unsigned char *)name);
    if (nname == NULL) {
        DEBUG_ASSIGN_ERROR(err, NC_ENOMEM)
        goto err_check;
    }

    /* check whether new name is already in use, for this API (def_var) the
     * name should NOT already exist */
    err = ncmpii_NC_findvar(&ncp->vars, nname, NULL);
    if (err != NC_ENOTVAR) {
        DEBUG_ASSIGN_ERROR(err, NC_ENAMEINUSE)
        goto err_check;
    }
    else
        err = NC_NOERR;

err_check:
    if (ncp->safe_mode) {
        int root_name_len, root_ndims, status, mpireturn;
        char *root_name;

        /* check if name is consistent among all processes */
        root_name_len = 1;
        if (name != NULL) root_name_len += strlen(name);
        TRACE_COMM(MPI_Bcast)(&root_name_len, 1, MPI_INT, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (nname != NULL) free(nname);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast root_name_len");
        }

        root_name = (char*) NCI_Malloc((size_t)root_name_len);
        root_name[0] = '\0';
        if (name != NULL) strcpy(root_name, name);
        TRACE_COMM(MPI_Bcast)(root_name, root_name_len, MPI_CHAR, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (nname != NULL) free(nname);
            NCI_Free(root_name);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        }
        if (err == NC_NOERR && strcmp(root_name, name))
            DEBUG_ASSIGN_ERROR(err, NC_EMULTIDEFINE_VAR_NAME)
        NCI_Free(root_name);

        /* check if type is consistent among all processes */
        nc_type root_type=type;
        TRACE_COMM(MPI_Bcast)(&root_type, 1, MPI_INT, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (nname != NULL) free(nname);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        }
        if (err == NC_NOERR && root_type != type)
            DEBUG_ASSIGN_ERROR(err, NC_EMULTIDEFINE_VAR_TYPE)

        /* check if ndims is consistent among all processes */
        root_ndims=ndims;
        TRACE_COMM(MPI_Bcast)(&root_ndims, 1, MPI_INT, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (nname != NULL) free(nname);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        }
        if (err == NC_NOERR && root_ndims != ndims)
            DEBUG_ASSIGN_ERROR(err, NC_EMULTIDEFINE_VAR_NDIMS)

        /* check if dimids is consistent among all processes */
        if (root_ndims > 0) {
            int *root_dimids = (int*)NCI_Malloc((size_t)root_ndims * SIZEOF_INT);
            if (dimids != NULL)
                memcpy(root_dimids, dimids, (size_t)root_ndims*SIZEOF_INT);
            else
                memset(root_dimids, 0, (size_t)root_ndims*SIZEOF_INT);
            TRACE_COMM(MPI_Bcast)(root_dimids, root_ndims, MPI_INT, 0, ncp->comm);
            if (mpireturn != MPI_SUCCESS) {
                if (nname != NULL) free(nname);
                NCI_Free(root_dimids);
                return ncmpii_handle_error(mpireturn, "MPI_Bcast");
            }
            if (err == NC_NOERR && dimids != NULL &&
                memcmp(root_dimids, dimids, (size_t)root_ndims*SIZEOF_INT))
                DEBUG_ASSIGN_ERROR(err, NC_EMULTIDEFINE_VAR_DIMIDS)
            NCI_Free(root_dimids);
        }

        /* find min error code across processes */
        TRACE_COMM(MPI_Allreduce)(&err, &status, 1, MPI_INT, MPI_MIN, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (nname != NULL) free(nname);
            return ncmpii_handle_error(mpireturn, "MPI_Allreduce");
        }
        if (err == NC_NOERR) err = status;
    }

    if (err != NC_NOERR) {
        if (nname != NULL) free(nname);
        return err;
    }

    assert(nname != NULL);

    /* create a new variable */
    err = ncmpii_new_NC_var(&ncp->vars, nname, type, ndims, dimids, &varp);
    free(nname);
    if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)

    /* set up array dimensional structures */
    err = ncmpii_NC_var_shape64(varp, &ncp->dims);
    if (err != NC_NOERR) {
        ncmpii_free_NC_var(varp);
        DEBUG_RETURN_ERROR(err)
    }

    /* Add a new handle to the end of an array of handles */
    err = incr_NC_vararray(&ncp->vars, varp);
    if (err != NC_NOERR) {
        ncmpii_free_NC_var(varp);
        DEBUG_RETURN_ERROR(err)
    }

    assert(varp != NULL);

    /* ncp->vars.ndefined has been increased in incr_NC_vararray() */
    varp->varid = (int)ncp->vars.ndefined - 1; /* varid */

    if (varidp != NULL) *varidp = varp->varid;

    /* default is NOFILL */
    varp->no_fill = 1;

    /* change to FILL only if the entire dataset fill mode is FILL */
    if (NC_dofill(ncp)) varp->no_fill = 0;

    return NC_NOERR;
}


/*----< ncmpii_inq_varid() >-------------------------------------------------*/
/* This is an independent subroutine */
int
ncmpii_inq_varid(void       *ncdp,
                 const char *name,
                 int        *varid)
{
    int err=NC_NOERR;
    char *nname=NULL; /* normalized name */
    NC *ncp=(NC*)ncdp;

    if (name == NULL || *name == 0 || strlen(name) > NC_MAX_NAME)
        DEBUG_RETURN_ERROR(NC_EBADNAME)

    /* create a normalized character string */
    nname = (char *)ncmpii_utf8proc_NFC((const unsigned char *)name);
    if (nname == NULL) DEBUG_RETURN_ERROR(NC_ENOMEM)

    err = ncmpii_NC_findvar(&ncp->vars, nname, varid);
    free(nname);
    if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)

    return NC_NOERR;
}

/*----< ncmpii_inq_var() >---------------------------------------------------*/
/* This is an independent subroutine */
int
ncmpii_inq_var(void       *ncdp,
               int         varid,
               char       *name,
               nc_type    *xtypep,
               int        *ndimsp,
               int        *dimids,
               int        *nattsp,
               MPI_Offset *offsetp,
               int        *no_fillp,    /* OUT: 1 not fill mode, 0 fill mode */
               void       *fill_valuep) /* OUT: user-defined or default fill value */
{
    int err=NC_NOERR;
    NC *ncp=(NC*)ncdp;
    NC_var *varp=NULL;

    /* using NC_GLOBAL in varid is illegal for this API, except for inquiring
     * natts. See
     * http://www.unidata.ucar.edu/mailing_lists/archives/netcdfgroup/2015/msg00196.html
     * Checking NC_GLOBAL has been done at the calling routines at top level.
    if (varid == NC_GLOBAL) DEBUG_RETURN_ERROR(NC_EGLOBAL)
     */

    if (varid == NC_GLOBAL) {
        /* in this case, all other pointer arguments must be NULLs */
        if (nattsp != NULL)
            *nattsp = (int) ncp->attrs.ndefined;
        return NC_NOERR;
    }

    varp = elem_NC_vararray(&ncp->vars, varid);
    if (varp == NULL) DEBUG_RETURN_ERROR(NC_ENOTVAR)

    if (name != NULL)
        /* in PnetCDF, name->cp is always NULL character terminated */
        strcpy(name, varp->name->cp);

    if (xtypep != NULL)
        *xtypep = varp->type;

    if (ndimsp != NULL) {
#ifdef ENABLE_SUBFILING
        /* varp->num_subfiles is already set during open or enddef */
        if (varp->num_subfiles > 1)
            *ndimsp = varp->ndims_org;
        else
#endif
            *ndimsp = varp->ndims;
    }
    if (dimids != NULL) {
#ifdef ENABLE_SUBFILING
        /* varp->dimids_org is already set during open or enddef */
        if (varp->num_subfiles > 1)
            memcpy(dimids, varp->dimids_org, (size_t)varp->ndims_org * SIZEOF_INT);
        else
#endif
            memcpy(dimids, varp->dimids, (size_t)varp->ndims * SIZEOF_INT);
    }
    if (nattsp != NULL)
        *nattsp = (int) varp->attrs.ndefined;

    if (offsetp != NULL) *offsetp = varp->begin;

    if (no_fillp != NULL) *no_fillp = varp->no_fill;

    if (fill_valuep != NULL) {
        err = ncmpii_inq_var_fill(varp, fill_valuep);
        if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)
    }

    return NC_NOERR;
}


/*----< ncmpii_rename_var() >-------------------------------------------------*/
/* This API is collective.
 * If the new name is longer than the old name, the netCDF file must be in
 * define mode. Otherwise, it can be called in either define or data mode.
 */
int
ncmpii_rename_var(void       *ncdp,
                  int         varid,
                  const char *newname)
{
    int err=NC_NOERR;
    char *nnewname=NULL; /* normalized name */
    NC *ncp=(NC*)ncdp;
    NC_var *varp=NULL;
    NC_string *newStr=NULL;

    /* check whether file's write permission */
    if (NC_readonly(ncp)) {
        DEBUG_ASSIGN_ERROR(err, NC_EPERM)
        goto err_check;
    }

    /* check whether variable ID is valid */
    err = ncmpii_NC_lookupvar(ncp, varid, &varp);
    if (err != NC_NOERR) {
        DEBUG_TRACE_ERROR
        goto err_check;
    }

    if (newname == NULL || *newname == 0) {
        DEBUG_ASSIGN_ERROR(err, NC_EBADNAME)
        goto err_check;
    }

    if (strlen(newname) > NC_MAX_NAME) {
        DEBUG_ASSIGN_ERROR(err, NC_EMAXNAME)
        goto err_check;
    }

    /* check whether new name is legal */
    err = ncmpii_NC_check_name(newname, ncp->format);
    if (err != NC_NOERR) {
        DEBUG_TRACE_ERROR
        goto err_check;
    }

    /* create a normalized character string */ 
    nnewname = (char *)ncmpii_utf8proc_NFC((const unsigned char *)newname);
    if (nnewname == NULL) {
        DEBUG_ASSIGN_ERROR(err, NC_ENOMEM)
        goto err_check;
    }

    /* check whether new name is already in use, for this API (rename) the
     * name should NOT already exist */
    err = ncmpii_NC_findvar(&ncp->vars, nnewname, NULL);
    if (err != NC_ENOTVAR) { /* expecting NC_ENOTVAR */
        DEBUG_ASSIGN_ERROR(err, NC_ENAMEINUSE)
        goto err_check;
    }
    else err = NC_NOERR;  /* reset err */

    if (! NC_indef(ncp) && /* when file is in data mode */
        varp->name->nchars < (MPI_Offset)strlen(nnewname)) {
        /* must in define mode when newname is longer */
        DEBUG_ASSIGN_ERROR(err, NC_ENOTINDEFINE)
        goto err_check;
    }

    newStr = ncmpii_new_NC_string(strlen(nnewname), nnewname);
    if (newStr == NULL) {
        DEBUG_ASSIGN_ERROR(err, NC_ENOMEM)
        goto err_check;
    }

#ifndef SEARCH_NAME_LINEARLY
    /* update var name lookup table */
    err = ncmpii_update_name_lookup_table(ncp->vars.nameT, varid,
          ncp->vars.value[varid]->name->cp, nnewname);
    if (err != NC_NOERR) {
        DEBUG_TRACE_ERROR
        goto err_check;
    }
#endif

err_check:
    if (nnewname != NULL) free(nnewname);
    if (ncp->safe_mode) {
        int root_name_len, root_varid, status, mpireturn;
        char *root_name;

        /* check if newname is consistent among all processes */
        root_name_len = 1;
        if (newname != NULL) root_name_len += strlen(newname);
        TRACE_COMM(MPI_Bcast)(&root_name_len, 1, MPI_INT, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (newStr != NULL) ncmpii_free_NC_string(newStr);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast root_name_len");
        }

        root_name = (char*) NCI_Malloc((size_t)root_name_len);
        root_name[0] = '\0';
        if (newname != NULL) strcpy(root_name, newname);
        TRACE_COMM(MPI_Bcast)(root_name, root_name_len, MPI_CHAR, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (newStr != NULL) ncmpii_free_NC_string(newStr);
            NCI_Free(root_name);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        }
        if (err == NC_NOERR && strcmp(root_name, newname))
            DEBUG_ASSIGN_ERROR(err, NC_EMULTIDEFINE_VAR_NAME)
        NCI_Free(root_name);

        /* check if varid is consistent across all processes */
        root_varid = varid;
        TRACE_COMM(MPI_Bcast)(&root_varid, 1, MPI_INT, 0, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (newStr != NULL) ncmpii_free_NC_string(newStr);
            return ncmpii_handle_error(mpireturn, "MPI_Bcast");
        }
        if (err == NC_NOERR && root_varid != varid)
            DEBUG_ASSIGN_ERROR(err, NC_EMULTIDEFINE_FNC_ARGS)

        /* find min error code across processes */
        TRACE_COMM(MPI_Allreduce)(&err, &status, 1, MPI_INT, MPI_MIN, ncp->comm);
        if (mpireturn != MPI_SUCCESS) {
            if (newStr != NULL) ncmpii_free_NC_string(newStr);
            return ncmpii_handle_error(mpireturn, "MPI_Allreduce");
        }
        if (err == NC_NOERR) err = status;
    }

    if (err != NC_NOERR) {
        if (newStr != NULL) ncmpii_free_NC_string(newStr);
        return err;
    }

    assert(varp != NULL);

    /* replace the old name with new name */
    ncmpii_free_NC_string(varp->name);
    varp->name = newStr;

    if (! NC_indef(ncp)) { /* when file is in data mode */
        /* Let root write the entire header to the file. Note that we cannot
         * just update the variable name in its space occupied in the file
         * header, because if the file space occupied by the name shrinks, all
         * the metadata following it must be moved ahead.
         */
        err = ncmpii_write_header(ncp);
        if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)
    }

    return err;
}

#ifdef __DEBUG

/*----< ncmpi_print_all_var_offsets() >---------------------------------------*/
/* This is an independent subroutine */
int
ncmpi_print_all_var_offsets(int ncid) {
    int i, err;
    NC_var **vpp=NULL;
    NC *ncp=NULL;
    PNC *pncp;

    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) DEBUG_RETURN_ERROR(err)
    ncp = (NC*)pncp->ncp;

    if (ncp->begin_var%1048576)
        printf("%s header size (ncp->begin_var)=%lld MB + %lld\n",
        ncp->path, ncp->begin_var/1048575, ncp->begin_var%1048576);
    else
        printf("%s header size (ncp->begin_var)=%lld MB\n",
        ncp->path, ncp->begin_var/1048575);

    vpp = ncp->vars.value;
    for (i=0; i<ncp->vars.ndefined; i++, vpp++) {
        char str[1024];
        MPI_Offset off = (*vpp)->begin;
        MPI_Offset rem = off % 1048576;;

        if (IS_RECVAR(*vpp))
            sprintf(str,"    Record variable \"%20s\": ",(*vpp)->name->cp);
        else
            sprintf(str,"non-record variable \"%20s\": ",(*vpp)->name->cp);

        if (rem)
            printf("%s offset=%12lld MB + %7lld len=%lld\n", str, off/1048576, rem,(*vpp)->len);
        else
            printf("%s offset=%12lld MB len=%lld\n", str, off/1048576,(*vpp)->len);
    }
    return NC_NOERR;
}

#endif