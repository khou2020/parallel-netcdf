dnl Process this m4 file to produce 'C' language file.
dnl
dnl If you see this line, you can ignore the next one.
/* Do not edit this file. It is produced from the corresponding .m4 source */
dnl
/*
 *  Copyright (C) 2003, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#if HAVE_CONFIG_H
# include <ncconfig.h>
#endif

#include <stdio.h>
#include <unistd.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#include <string.h> /* memcpy() */
#include <assert.h>

#include <mpi.h>

#include "nc.h"
#include "ncx.h"
#include "ncmpidtype.h"
#include "macro.h"
#ifdef ENABLE_SUBFILING
#include "subfile.h"
#endif


/*----< ncmpii_calc_datatype_elems() >---------------------------------------*/
/* obtain the following metadata about buftype:
 * ptype: element data type (MPI primitive type) in buftype
 * bufcount: If it is -1, then this is called from a high-level API and in
 * this case buftype will be an MPI primitive data type. If not -1, then this
 * is called from a flexible API. In that case, we recalculate bufcount to
 * match with count[].
 * bnelems: number of ptypes in user buffer
 * nbytes: number of bytes (in external data representation) to read/write
 * from/to the file
 * el_size: size of ptype
 * buftype_is_contig: whether buftype is contiguous
 */
int
ncmpii_calc_datatype_elems(NC_var           *varp,
                           const MPI_Offset *count,
                           MPI_Datatype      buftype,
                           MPI_Datatype     *ptype,             /* out */
                           MPI_Offset       *bufcount,          /* in/out */
                           MPI_Offset       *bnelems,           /* out */
                           MPI_Offset       *nbytes,            /* out */
                           int              *el_size,           /* out */
                           int              *buftype_is_contig) /* out */
{
    int i, err=NC_NOERR;
    MPI_Offset fnelems;

    /* Sanity check for error codes should have already done before reaching
     * here
     *
     * when (*bufcount == -1), same as if (IsPrimityMPIType(buftype)),
     * it means this subroutine is called from a high-level API.
     */
    if (*bufcount != -1 && buftype != MPI_DATATYPE_NULL) {
        /* This subroutine is called from a flexible API */
        int isderived;
        /* check MPI derived datatype error */
        err = ncmpii_dtype_decode(buftype, ptype, el_size, bnelems,
                                  &isderived, buftype_is_contig);
        if (err != NC_NOERR) return err;
    }

    /* fnelems is the total number of nc_type elements calculated from
     * count[]. count[] is the access count to the variable defined in
     * the netCDF file.
     */
    fnelems = 1;
    for (i=0; i<varp->ndims; i++)
        fnelems *= count[i];

    if (*bufcount == -1) { /* if (IsPrimityMPIType(buftype)) */
        /* this subroutine is called from a high-level API */
        *bnelems = *bufcount = fnelems;
        *ptype = buftype;
        MPI_Type_size(buftype, el_size); /* buffer element size */
        *buftype_is_contig = 1;
        /* nbytes is the amount in bytes of this request to file */
        *nbytes = *bnelems * varp->xsz; /* varp->xsz is external element size */
    }
    else if (buftype == MPI_DATATYPE_NULL) {
        /* This is called from a flexible API and buftype is set by user
         * to MPI_DATATYPE_NULL. In this case, bufcount is set to match
         * count[], and buf's data type to match the data type of variable
         * defined in the file - no data conversion will be done.
         */
        *bnelems = *bufcount = fnelems;
        *ptype = buftype = ncmpii_nc2mpitype(varp->type);
        *el_size = varp->xsz;
        *buftype_is_contig = 1;
        /* nbytes is the amount in bytes of this request to file */
        *nbytes = *bnelems * varp->xsz;
    }
    else {
        /* This is called from a flexible API */

        /* make bnelems the number of ptype in the whole user buf */
        *bnelems *= *bufcount;

        /* check mismatch between bnelems and fnelems */
        if (fnelems != *bnelems) {
            DEBUG_ASSIGN_ERROR(err, NC_EIOMISMATCH)
            (fnelems>*bnelems) ? (fnelems=*bnelems) : (*bnelems=fnelems);
            /* only handle partial of the request, smaller number of the two */
        }
        /* now fnelems == *bnelems */

        /* nbytes is the amount in bytes of this request to file */
        *nbytes = *bnelems * varp->xsz;
    }
    return err;
}

/*----< ncmpii_create_imaptype() >-------------------------------------------*/
/* Check if a request is a true varm call. If yes, create an MPI derived
 * data type, imaptype, using imap[]
 */
int
ncmpii_create_imaptype(NC_var           *varp,
                       const MPI_Offset *count,
                       const MPI_Offset *imap,
                       const MPI_Offset  bnelems, /* no. elements in user buf */
                       const int         el_size, /* user buf element size */
                       MPI_Datatype      ptype,   /* element type in buftype */
                       MPI_Datatype     *imaptype)/* out */
{
    int dim, mpireturn;
    MPI_Offset imap_contig_blocklen;

    /* check if this is a vars call or a true varm call */
    *imaptype = MPI_DATATYPE_NULL;

    if (varp->ndims == 0) /* scalar var, only one value at one fixed place */
        return NC_NOERR;

    if (bnelems == 1) /* one element, same as var1 */
        return NC_NOERR;

    if (imap == NULL) /* no mapping, same as vars */
        return NC_NOERR;

    /* test each dim's contiguity in imap[] until the 1st non-contiguous
     * dim is reached */
    imap_contig_blocklen = 1;
    dim = varp->ndims;
    while (--dim >= 0 && imap_contig_blocklen == imap[dim])
        imap_contig_blocklen *= count[dim];

    if (dim == -1) /* imap is a contiguous layout */
        return NC_NOERR;

    /* We have a true varm call, as imap gives non-contiguous layout.
     * User buffer will be packed (write case) or unpacked (read case)
     * to/from a contiguous buffer based on imap[], before MPI-IO.
     * First, we construct a derived data type, imaptype, based on
     * imap[], and use it to pack lbuf to cbuf (for write), or unpack
     * cbuf to lbuf (for read).
     * dim is the first dimension (C order, eg. ZYX) that has
     * non-contiguous imap.
     */
    if (imap_contig_blocklen != (int)imap_contig_blocklen)
        DEBUG_RETURN_ERROR(NC_EINTOVERFLOW)
    if (count[dim] != (int)count[dim] || imap[dim] != (int)imap[dim])
        DEBUG_RETURN_ERROR(NC_EINTOVERFLOW)

    mpireturn = MPI_Type_vector((int)count[dim], (int)imap_contig_blocklen,
                                (int)imap[dim], ptype, imaptype);
    if (mpireturn != MPI_SUCCESS) {
        ncmpii_handle_error(mpireturn,"MPI_Type_vector");
        DEBUG_RETURN_ERROR(NC_EMPI)
    }
    mpireturn = MPI_Type_commit(imaptype);
    if (mpireturn != MPI_SUCCESS) {
        ncmpii_handle_error(mpireturn,"MPI_Type_commit");
        DEBUG_RETURN_ERROR(NC_EMPI)
    }

    for (dim--; dim>=0; dim--) {
        MPI_Datatype tmptype;
        if (count[dim] != (int)count[dim])
            DEBUG_RETURN_ERROR(NC_EINTOVERFLOW)
#ifdef HAVE_MPI_TYPE_CREATE_HVECTOR
        mpireturn = MPI_Type_create_hvector((int)count[dim], 1,
                    imap[dim]*el_size, *imaptype, &tmptype);
        if (mpireturn != MPI_SUCCESS) {
            ncmpii_handle_error(mpireturn,"MPI_Type_create_hvector");
            DEBUG_RETURN_ERROR(NC_EMPI)
        }
#else
        mpireturn = MPI_Type_hvector((int)count[dim], 1, imap[dim]*el_size,
                    *imaptype, &tmptype);
        if (mpireturn != MPI_SUCCESS) {
            ncmpii_handle_error(mpireturn,"MPI_Type_hvector");
            DEBUG_RETURN_ERROR(NC_EMPI)
        }
#endif
        mpireturn = MPI_Type_free(imaptype);
        if (mpireturn != MPI_SUCCESS) {
            ncmpii_handle_error(mpireturn,"MPI_Type_free");
            DEBUG_RETURN_ERROR(NC_EMPI)
        }
        mpireturn = MPI_Type_commit(&tmptype);
        if (mpireturn != MPI_SUCCESS) {
            ncmpii_handle_error(mpireturn,"MPI_Type_commit");
            DEBUG_RETURN_ERROR(NC_EMPI)
        }
        *imaptype = tmptype;
    }
    return NC_NOERR;
}

/*----< ncmpii_getput_varm() >------------------------------------------------*/
/* buffer layers:

   User Level              buf     (user defined buffer of MPI_Datatype)
   MPI Datatype Level      cbuf    (contiguous buffer of ptype)
   NetCDF XDR Level        xbuf    (XDR I/O buffer)

   varm: there maybe two layer of memory layout (remapping):
         one is specified by MPI derived datatype,
         the other is specified by imap[],
         it's encouraged to use only one option of them,
         though using both of them are supported.

   user buffer:                         |--------------------------|

   mpi derived datatype view:           |------|  |------|  |------|

   logic (contig) memory datastream:       |------|------|------|

   imap view:                              |--| |--|    |--| |--|

   contig I/O datastream (internal represent): |--|--|--|--|

   These two layers of memory layout will both be represented in MPI
   derived datatype, and if double layers of memory layout is used,
   we need to eliminate the upper one passed in MPI_Datatype parameter
   from the user, by packing it to logic contig memory datastream view.

   for put_varm:
     1. pack buf to lbuf based on buftype
     2. create imap_type based on imap
     3. pack lbuf to cbuf based on imap_type
     4. type convert and byte swap cbuf to xbuf
     5. write from xbuf
     6. byte swap the buf, if it is swapped
     7. free up temp buffers

   for get_varm:
     1. allocate lbuf
     2. create imap_type based on imap
     3. allocate cbuf
     4. allocate xbuf
     5. read to xbuf
     6. type convert and byte swap xbuf to cbuf
     7. unpack cbuf to lbuf based on imap_type
     8. unpack lbuf to buf based on buftype
*/

static int
ncmpii_getput_varm(NC               *ncp,
                   NC_var           *varp,
                   const MPI_Offset  start[],
                   const MPI_Offset  count[],
                   const MPI_Offset  stride[],  /* can be NULL */
                   const MPI_Offset  imap[],    /* can be NULL */
                   void             *buf,
                   MPI_Offset        bufcount,  /* -1: from high-level API */
                   MPI_Datatype      buftype,
                   int               rw_flag,   /* WRITE_REQ or READ_REQ */
                   int               io_method) /* COLL_IO or INDEP_IO */
{
    void *lbuf=NULL, *cbuf=NULL, *xbuf=NULL;
    int mpireturn, err=NC_NOERR, status=NC_NOERR, warning=NC_NOERR;
    int el_size, buftype_is_contig;
    int need_swap=0, need_convert=0, need_swap_back_buf=0;
    MPI_Offset bnelems=0, nbytes=0, offset=0;
    MPI_Status mpistatus;
    MPI_Datatype ptype, filetype=MPI_BYTE, imaptype=MPI_DATATYPE_NULL;
    MPI_File fh;

#ifdef ENABLE_SUBFILING
    /* call a separate routine if variable is stored in subfiles */
    if (varp->num_subfiles > 1) {
#ifdef SUBFILE_DEBUG
        printf("var(%s) is stored in subfiles\n", varp->name->cp);
#endif
        if (imap != NULL) {
            fprintf(stderr, "varm APIs for subfiling is yet to be implemented\n");
            DEBUG_RETURN_ERROR(NC_ENOTSUPPORT)
        }
        
        return ncmpii_subfile_getput_vars(ncp, varp, start, count, stride,
                                          buf, bufcount, buftype,
                                          rw_flag, io_method);
    }
#endif

    /* calculate the followings:
     * ptype: element data type (MPI primitive type) in buftype
     * bufcount: If it is -1, then this is called from a high-level API and in
     * this case buftype will be an MPI primitive data type. If not, then this
     * is called from a flexible API. In that case, we recalculate bufcount to
     * match with count[].
     * bnelems: number of ptypes in user buffer
     * nbytes: number of bytes (in external data representation) to read/write
     * from/to the file
     * el_size: size of ptype
     * buftype_is_contig: whether buftype is contiguous
     */
    err = ncmpii_calc_datatype_elems(varp, count,
                                     buftype, &ptype, &bufcount, &bnelems,
                                     &nbytes, &el_size, &buftype_is_contig);
    if (err == NC_EIOMISMATCH) DEBUG_ASSIGN_ERROR(warning, err) 
    else if (err != NC_NOERR) goto err_check;

    /* because nbytes will be used as the argument "count" in MPI-IO
     * read/write calls and the argument "count" is of type int */
    if (nbytes != (int)nbytes) {
        DEBUG_ASSIGN_ERROR(err, NC_EINTOVERFLOW)
        goto err_check;
    }

    if (nbytes == 0) /* this process has nothing to read/write */
        goto err_check;

    /* TODO: if record variables are too big (so big that we cannot store the
     * stride between records in an MPI_Aint, for example) then we will
     * have to process this one record at a time.
     */

    /* Create the filetype for this request and calculate the beginning
     * file offset for this request.  If this request is contiguous in file,
     * then filetype == MPI_BYTE. Otherwise filetype will be an MPI derived
     * data type.
     */
    err = ncmpii_vars_create_filetype(ncp, varp, start, count, stride, rw_flag,
                                      NULL, &offset, &filetype, NULL);
    if (err != NC_NOERR) goto err_check;

    if (bufcount != (int)bufcount) DEBUG_ASSIGN_ERROR(err, NC_EINTOVERFLOW)

err_check:
    /* If io_method is COLL_IO and an error occurs, we'll still conduct a
     * zero-byte read/write (because every process must participate the
     * collective I/O call).
     */
    if (err != NC_NOERR || nbytes == 0) {
        if (io_method == INDEP_IO) return err;

        /* COLL_IO: participate the collective I/O operations */
        filetype = MPI_BYTE;
        status   = err;
        nbytes   = 0;
        goto mpi_io;
    }

    /* check if type conversion and Endianness byte swap is needed */
    need_convert = ncmpii_need_convert(ncp->format, varp->type, ptype);
    need_swap    = ncmpii_need_swap(varp->type, ptype);

    /* Check if this is a vars call or a true varm call.
     * Construct a derived datatype, imaptype, if a true varm call
     */
    err = ncmpii_create_imaptype(varp, count, imap, bnelems, el_size, ptype,
                                 &imaptype);
    if (status == NC_NOERR) status = err;

    if (rw_flag == WRITE_REQ) { /* pack request to xbuf */
        int position;
        MPI_Offset outsize=bnelems*el_size;
        /* assert(bnelems > 0); */
        if (outsize != (int)outsize && status == NC_NOERR)
            DEBUG_ASSIGN_ERROR(status, NC_EINTOVERFLOW)

        /* Step 1: pack buf into a contiguous buffer, lbuf */
        if (!buftype_is_contig) { /* buftype is not contiguous */
            /* pack buf into lbuf, a contiguous buffer, using buftype */
            lbuf = NCI_Malloc((size_t)outsize);
            position = 0;
            MPI_Pack(buf, (int)bufcount, buftype, lbuf, (int)outsize,
                     &position, MPI_COMM_SELF);
        }
        else
            lbuf = buf;

        /* Step 2: pack lbuf to cbuf if imap is non-contiguous */
        if (imaptype != MPI_DATATYPE_NULL) { /* true varm */
            /* pack lbuf to cbuf, a contiguous buffer, using imaptype */
            cbuf = NCI_Malloc((size_t)outsize);
            position = 0;
            MPI_Pack(lbuf, 1, imaptype, cbuf, (int)outsize, &position,
                     MPI_COMM_SELF);
            MPI_Type_free(&imaptype);
        }
        else /* reuse lbuf */
            cbuf = lbuf;

        /* lbuf is no longer needed */
        if (lbuf != buf && lbuf != cbuf) NCI_Free(lbuf);

        /* Step 3: pack cbuf to xbuf. The contents of xbuf will be in the
         * external representation, ready to be written to file.
         */
        xbuf = cbuf;
        if (need_convert) { /* user buf type != nc var type defined in file */
            void *fillp; /* fill value in internal representation */

            xbuf = NCI_Malloc((size_t)nbytes);

            /* find the fill value */
            fillp = NCI_Malloc((size_t)varp->xsz);
            ncmpii_inq_var_fill(varp, fillp);
            /* datatype conversion + byte-swap from cbuf to xbuf */
            DATATYPE_PUT_CONVERT(ncp->format, varp->type, xbuf, cbuf, bnelems,
                                 ptype, fillp, status)
            NCI_Free(fillp);

            /* NC_ERANGE can be caused by a subset of buf that is out of range
             * of the external data type, it is not considered a fatal error.
             * The request must continue to finish.
             */
            if (status != NC_NOERR && status != NC_ERANGE) {
                if (cbuf != buf)  NCI_Free(cbuf);
                NCI_Free(xbuf);
                xbuf = NULL;
                if (io_method == INDEP_IO) return status;

                /* COLL_IO: participate the collective I/O operations */
                filetype  = MPI_BYTE;
                nbytes    = 0;
                goto mpi_io;
            }
        }
        else if (need_swap) { /* no need to convert, just byte swap */
#ifdef DISABLE_IN_PLACE_SWAP
            if (cbuf == buf)
#else
            if (cbuf == buf && nbytes <= NC_BYTE_SWAP_BUFFER_SIZE)
#endif
            {
                /* allocate cbuf and copy buf to xbuf, before byte-swap */
                xbuf = NCI_Malloc((size_t)nbytes);
                memcpy(xbuf, buf, (size_t)nbytes);
            }

            /* perform array in-place byte-swap on xbuf */
            ncmpii_in_swapn(xbuf, bnelems, ncmpix_len_nctype(varp->type));

            if (xbuf == buf) need_swap_back_buf = 1;
            /* user buf needs to be swapped back to its original contents */
        }
        /* cbuf is no longer needed */
        if (cbuf != buf && cbuf != xbuf) NCI_Free(cbuf);
    }
    else { /* rw_flag == READ_REQ */
        /* allocate xbuf for reading */
        if (buftype_is_contig && imaptype == MPI_DATATYPE_NULL && !need_convert)
            xbuf = buf;
        else
            xbuf = NCI_Malloc((size_t)nbytes);
    }
    /* xbuf is the buffer whose data has been converted into the external
     * data type, ready to be written to the netCDF file. For read,
     * after read from file, the contents of xbuf are in external type
     */

mpi_io:
    if (io_method == COLL_IO)
        fh = ncp->nciop->collective_fh;
    else
        fh = ncp->nciop->independent_fh;

    /* MPI_File_set_view is collective */
    err = ncmpii_file_set_view(ncp, fh, &offset, filetype);
    if (err != NC_NOERR) {
        nbytes = 0; /* skip this request */
        if (status == NC_NOERR) status = err;
    }
    if (filetype != MPI_BYTE) MPI_Type_free(&filetype);

    if (rw_flag == WRITE_REQ) {
        if (io_method == COLL_IO) {
            TRACE_IO(MPI_File_write_at_all)(fh, offset, xbuf, (int)nbytes,
                                            MPI_BYTE, &mpistatus);
            if (mpireturn != MPI_SUCCESS) {
                err = ncmpii_handle_error(mpireturn, "MPI_File_write_at_all");
                /* return the first encountered error if there is any */
                if (status == NC_NOERR) {
                    err = (err == NC_EFILE) ? NC_EWRITE : err;
                    DEBUG_ASSIGN_ERROR(status, err)
                }
            }
            else {
                ncp->nciop->put_size += nbytes;
            }
        }
        else { /* io_method == INDEP_IO */
            TRACE_IO(MPI_File_write_at)(fh, offset, xbuf, (int)nbytes,
                                        MPI_BYTE,  &mpistatus);
            if (mpireturn != MPI_SUCCESS) {
                err = ncmpii_handle_error(mpireturn, "MPI_File_write_at");
                /* return the first encountered error if there is any */
                if (status == NC_NOERR) {
                    err = (err == NC_EFILE) ? NC_EWRITE : err;
                    DEBUG_ASSIGN_ERROR(status, err)
                }
            }
            else {
                ncp->nciop->put_size += nbytes;
            }
        }
    }
    else {  /* rw_flag == READ_REQ */
        if (io_method == COLL_IO) {
            TRACE_IO(MPI_File_read_at_all)(fh, offset, xbuf, (int)nbytes,
                                           MPI_BYTE, &mpistatus);
            if (mpireturn != MPI_SUCCESS) {
                err = ncmpii_handle_error(mpireturn, "MPI_File_read_at_all");
                /* return the first encountered error if there is any */
                if (status == NC_NOERR) {
                    err = (err == NC_EFILE) ? NC_EREAD : err;
                    DEBUG_ASSIGN_ERROR(status, err)
                }
            }
            else {
                ncp->nciop->get_size += nbytes;
            }
        }
        else { /* io_method == INDEP_IO */
            TRACE_IO(MPI_File_read_at)(fh, offset, xbuf, (int)nbytes,
                                       MPI_BYTE, &mpistatus);
            if (mpireturn != MPI_SUCCESS) {
                err = ncmpii_handle_error(mpireturn, "MPI_File_read_at");
                /* return the first encountered error if there is any */
                if (status == NC_NOERR) {
                    err = (err == NC_EFILE) ? NC_EREAD : err;
                    DEBUG_ASSIGN_ERROR(status, err)
                }
            }
            else {
                ncp->nciop->get_size += nbytes;
            }
        }
    }

    /* No longer need to reset the file view, as the root's fileview includes
     * the whole file header.
     TRACE_IO(MPI_File_set_view)(fh, 0, MPI_BYTE, MPI_BYTE, "native",
                                 MPI_INFO_NULL);
     */

    if (rw_flag == READ_REQ && nbytes > 0) {
        /* xbuf contains the data read from file.
         * Check if it needs to be type-converted + byte-swapped
         */
        int position;
        MPI_Offset insize=bnelems*el_size;
        if (insize != (int)insize && status == NC_NOERR)
            DEBUG_ASSIGN_ERROR(status, NC_EINTOVERFLOW)

        if (need_convert) {
            /* xbuf cannot be buf, but cbuf can */
            if (buftype_is_contig && imaptype == MPI_DATATYPE_NULL)
                cbuf = buf; /* vars call and buftype is contiguous */
            else
                cbuf = NCI_Malloc((size_t)insize);

            /* type conversion + byte-swap from xbuf to cbuf */
            DATATYPE_GET_CONVERT(ncp->format, varp->type, xbuf, cbuf, bnelems,
                                 ptype, err)

            /* retain the first error status */
            if (status == NC_NOERR) status = err;
            NCI_Free(xbuf);
        } else {
            if (need_swap) /* perform array in-place byte-swap on xbuf */
                ncmpii_in_swapn(xbuf, bnelems, ncmpix_len_nctype(varp->type));
            cbuf = xbuf;
        }
        /* done with xbuf */

        /* varm && noncontig: cbuf -> lbuf -> buf
           vars && noncontig: cbuf == lbuf -> buf
           varm && contig:    cbuf -> lbuf == buf
           vars && contig:    cbuf == lbuf == buf
        */
        if (imaptype != MPI_DATATYPE_NULL && !buftype_is_contig)
            /* a true varm and buftype is not contiguous: we need a separate
             * buffer, lbuf, to unpack cbuf to lbuf using imaptype, and later
             * unpack lbuf to buf using buftype.
             * In this case, cbuf cannot be buf and lbuf cannot be buf.
             */
            lbuf = NCI_Malloc((size_t)insize);
        else if (imaptype == MPI_DATATYPE_NULL) /* not varm */
            lbuf = cbuf;
        else /* varm and buftype is contiguous */
            lbuf = buf;

        if (imaptype != MPI_DATATYPE_NULL) {
            /* unpack cbuf to lbuf based on imaptype */
            position = 0;
            MPI_Unpack(cbuf, (int)insize, &position, lbuf, 1, imaptype,
                       MPI_COMM_SELF);
            MPI_Type_free(&imaptype);
        }
        /* done with cbuf */
        if (cbuf != lbuf) NCI_Free(cbuf);

        if (!buftype_is_contig) {
            /* unpack lbuf to buf based on buftype */
            position = 0;
            MPI_Unpack(lbuf, (int)insize, &position, buf, (int)bufcount,
                       buftype, MPI_COMM_SELF);
        }
        /* done with lbuf */
        if (lbuf != buf) NCI_Free(lbuf);
    }
    else if (rw_flag == WRITE_REQ) {
        if (xbuf != NULL && xbuf != buf) NCI_Free(xbuf);

        if (need_swap_back_buf) /* byte-swap back to buf's original contents */
            ncmpii_in_swapn(buf, bnelems, ncmpix_len_nctype(varp->type));

        if (IS_RECVAR(varp)) {
            /* update header's number of records in memory */
            MPI_Offset new_numrecs = ncp->numrecs;

            /* calculate the max record ID written by this request */
            if (status == NC_NOERR) { /* do this only if no error */
                if (stride == NULL)
                    new_numrecs = start[0] + count[0];
                else
                    new_numrecs = start[0] + (count[0] - 1) * stride[0] + 1;

                /* note new_numrecs can be smaller than ncp->numrecs */
            }

            if (io_method == INDEP_IO) {
                /* For independent put, we delay the sync for numrecs until
                 * the next collective call, such as end_indep(), sync(),
                 * enddef(), or close(). This is because if we update numrecs
                 * to file now, race condition can happen. Note numrecs in
                 * memory may be inconsistent and obsolete till then.
                 */
                if (ncp->numrecs < new_numrecs) {
                    ncp->numrecs = new_numrecs;
                    set_NC_ndirty(ncp);
                }
            }
            else { /* COLL_IO: sync numrecs in memory and file */
                /* In ncmpiio_sync_numrecs(), new_numrecs is checked against
                 * ncp->numrecs.
                 */
                err = ncmpiio_sync_numrecs(ncp, new_numrecs);
                if (status == NC_NOERR) status = err;
            }
        }

        if (NC_doFsync(ncp)) { /* NC_SHARE is set */
            TRACE_IO(MPI_File_sync)(fh);
            if (io_method == COLL_IO)
                TRACE_COMM(MPI_Barrier)(ncp->nciop->comm);
        }
    }

    return ((warning != NC_NOERR) ? warning : status);
}

include(`foreach.m4')dnl
include(`utils.m4')dnl
dnl
define(`APINAME',`ifelse(`$3',`',`ncmpi_$1_var$2$4',`ncmpi_$1_var$2_$3$4')')dnl
dnl
dnl GETPUT_API(get/put, kind, itype, coll/indep)
dnl
define(`GETPUT_API',dnl
`dnl
/*----< APINAME($1,$2,$3,$4)() >---------------------------------------------*/
int
APINAME($1,$2,$3,$4)(int ncid, int varid, ArgKind($2) BufArgs($1,$3))
{
    int         err, status;
    NC         *ncp;
    NC_var     *varp=NULL;
    ifelse(`$2', `',  `MPI_Offset *start, *count;',
           `$2', `1', `MPI_Offset *count;')

    status = ncmpii_sanity_check(ncid, varid, ArgStartCountStride($2),
                                 ifelse(`$3', `', `bufcount', `0'),
                                 ifelse(`$3', `', `buftype',  `ITYPE2MPI($3)'),
                                 API_KIND($2), ifelse(`$3', `', `1', `0'),
                                 1, ReadWrite($1),
                                 CollIndep($4), &ncp, &varp);
    if (status != NC_NOERR) {
        if (CollIndep($4) == INDEP_IO ||
            status == NC_EBADID ||
            status == NC_EPERM ||
            status == NC_EINDEFINE ||
            status == NC_EINDEP ||
            status == NC_ENOTINDEP)
            return status;  /* fatal error, cannot continue */

        /* for collective API, participate the collective I/O with zero-length
         * request for this process */
        err = ncmpii_getput_zero_req(ncp, ReadWrite($1));
        assert(err == NC_NOERR);

        /* return the error code from sanity check */
        return status;
    }

    ifelse(`$2', `',  `GET_FULL_DIMENSIONS(start, count)',
           `$2', `1', `GET_ONE_COUNT(count)')

    /* APINAME($1,$2,$3,$4) is a special case of APINAME($1,m,$3,$4) */
    status = ncmpii_getput_varm(ncp, varp, start, count, ArgStrideMap($2),
                                (void*)buf,
                                ifelse(`$3', `', `bufcount, buftype',
                                                 `-1, ITYPE2MPI($3)'),
                                ReadWrite($1), CollIndep($4));
    ifelse(`$2', `', `NCI_Free(start);', `$2', `1', `NCI_Free(count);')
    return status;
}
')dnl
dnl
/*---- PnetCDF flexible APIs ------------------------------------------------*/
foreach(`kind', (, 1, a, s, m),
        `foreach(`putget', (put, get),
                 `foreach(`collindep', (, _all),
                          `GETPUT_API(putget,kind,,collindep)'
)')')

/*---- PnetCDF high-level APIs ----------------------------------------------*/
foreach(`kind', (, 1, a, s, m),
        `foreach(`putget', (put, get),
                 `foreach(`collindep', (, _all),
                          `foreach(`itype', (ITYPE_LIST),
                                   `GETPUT_API(putget,kind,itype,collindep)'
)')')')