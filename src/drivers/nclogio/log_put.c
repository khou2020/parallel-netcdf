#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <nc.h>
#include <ncx.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <pnetcdf.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pnc_debug.h>
#include <common.h>
#include <pnetcdf.h>
#include <log.h>
#include <stdlib.h>
#include <stdio.h>

/*
 * Prepare a single log entry to be write to log
 * IN    nclogp:    log structure to log this entry
 * IN    varp:    NC_var structure associate to this entry
 * IN    start: start in put_var* call
 * IN    count: count in put_var* call
 * IN    stride: stride in put_var* call
 * IN    bur:    buffer of data to write
 * IN    buftype:    buftype as in ncmpio_getput_varm, MPI_PACKED indicate a flexible api
 * IN    PackedSize:    Size of buf in byte, only valid when buftype is MPI_PACKED
 */
int log_put_var(void *ncdp, int varid, const MPI_Offset *start, 
                    const MPI_Offset *count, const MPI_Offset *stride, 
                    const MPI_Offset *imap, const void *buf, 
                    MPI_Offset bufcount, MPI_Datatype buftype, 
                    api_kind api, nc_type itype) {
    void *lbuf=NULL, *cbuf=NULL, *xbuf=NULL;
    int mpireturn, err=NC_NOERR, status=NC_NOERR, warning=NC_NOERR;
    int el_size, buftype_is_contig;
    int need_swap=0, need_convert=0, need_swap_back_buf=0, position;
    MPI_Offset bnelems=0, nbytes=0, offset=0, outsize;
    MPI_Status mpistatus;
    MPI_Datatype ptype, filetype=MPI_BYTE, imaptype=MPI_DATATYPE_NULL;
    MPI_File fh;
    NC_Log *nclogp = (NC_Log*)ncdp;  
    NC *ncp = (NC*)nclogp->ncp;
    NC_var *varp;

    /* Retrive variable */
    err = ncmpio_NC_lookupvar(ncp, varid, &varp);
    if (err != NC_NOERR){
        return err;
    }

    /* calculate the followings:
     * ptype: element data type (MPI primitive type) in buftype
     * bufcount: If it is -1, then this is called from a high-level API and in
     * this case buftype will be an MPI primitive data type. If not, then this
     * is called from a flexible API. In the former case, we recalculate
     * bufcount to match with count[].
     * bnelems: number of ptypes in user buffer
     * nbytes: number of bytes (in external data representation) to read/write
     * from/to the file
     * el_size: size of ptype
     * buftype_is_contig: whether buftype is contiguous
     */
    err = ncmpio_calc_datatype_elems(varp, count,
                                     buftype, &ptype, &bufcount, &bnelems,
                                     &nbytes, &el_size, &buftype_is_contig);
    if (err == NC_EIOMISMATCH) DEBUG_ASSIGN_ERROR(warning, err) 
    else if (err != NC_NOERR) return err;

    /* because nbytes will be used as the argument "count" in MPI-IO
     * read/write calls and the argument "count" is of type int */
    if (nbytes != (int)nbytes) {
        DEBUG_ASSIGN_ERROR(err, NC_EINTOVERFLOW)
        DEBUG_RETURN_ERROR(err);
    }

    if (nbytes == 0) /* this process has nothing to read/write */
        return NC_NOERR;

    /* TODO: if record variables are too big (so big that we cannot store the
     * stride between records in an MPI_Aint, for example) then we will
     * have to process this one record at a time.
     */

    /* Create the filetype for this request and calculate the beginning
     * file offset for this request.  If this request is contiguous in file,
     * then filetype == MPI_BYTE. Otherwise filetype will be an MPI derived
     * data type.
     */
    err = ncmpio_filetype_create_vars(ncp, varp, start, count, stride, 
                                    WRITE_REQ, NULL, &offset, &filetype, NULL);
    if (err != NC_NOERR) return err;

    if (bufcount != (int)bufcount) DEBUG_ASSIGN_ERROR(err, NC_EINTOVERFLOW)

    /* check if type conversion and Endianness byte swap is needed */
    need_convert = ncmpio_need_convert(ncp->format, varp->type, ptype);
    need_swap    = ncmpio_need_swap(varp->type, ptype);

    /* Check if this is a vars call or a true varm call.
     * Construct a derived datatype, imaptype, if a true varm call
     */
    err = ncmpio_create_imaptype(varp, count, imap, bnelems, el_size, ptype,
                                 &imaptype);
    if (err != NC_NOERR) return err;

    outsize=bnelems*el_size;
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

    err = log_put(nclogp, varp, start, count, stride, cbuf, buftype, bnelems * el_size);

    /* Free the buffer */
    if (cbuf != buf){
        NCI_Free(cbuf);
    }

    return err;
}

/*
 * Prepare a single log entry to be write to log
 * IN    nclogp:    log structure to log this entry
 * IN    varp:    NC_var structure associate to this entry
 * IN    start: start in put_var* call
 * IN    count: count in put_var* call
 * IN    stride: stride in put_var* call
 * IN    bur:    buffer of data to write
 * IN    buftype:    buftype as in ncmpio_getput_varm, MPI_PACKED indicate a flexible api
 * IN    PackedSize:    Size of buf in byte, only valid when buftype is MPI_PACKED
 */
int log_put_varn(void *ncdp, int varid, int num, 
                    MPI_Offset* const *starts, MPI_Offset* const *counts, 
                    const void *buf, MPI_Offset bufcount, 
                    MPI_Datatype buftype, nc_type itype) {
    int i, j, el_size, status=NC_NOERR, min_st, err, free_cbuf=0;
    int req_id=NC_REQ_NULL, st, isSameGroup, position;
    void *cbuf=NULL;
    char *bufp;
    MPI_Offset packsize=0, **_counts=NULL;
    MPI_Datatype ptype;
    NC_Log *nclogp = (NC_Log*)ncdp;
    NC *ncp = (NC*)nclogp->ncp;
    NC_var *varp;

    /* Retrive variable */
    err = ncmpio_NC_lookupvar(nclogp->ncp, varid, &varp);
    if (err != NC_NOERR){
        return err;
    }
    
    /* check for zero-size request */
    if (num == 0 || bufcount == 0) return NC_NOERR;

    /* it is illegal for starts to be NULL */
    if (starts == NULL) {
        DEBUG_ASSIGN_ERROR(status, NC_ENULLSTART)
        goto err_check;
    }
    else { /* it is illegal for any starts[i] to be NULL */
        for (i=0; i<num; i++) {
            if (starts[i] == NULL) {
                DEBUG_ASSIGN_ERROR(status, NC_ENULLSTART)
                goto err_check;
            }
        }
    }

    if (buftype == MPI_DATATYPE_NULL) {
        /* In this case, bufcount is ignored and will be recalculated to match
         * counts[]. Note buf's data type must match the data type of
         * variable defined in the file - no data conversion will be done.
         */
        if (counts == NULL)
            bufcount = 1;
        else {
            bufcount = 0;
            for (j=0; j<num; j++) {
                MPI_Offset bufcount_j = 1;
                if (counts[i] == NULL) {
                    DEBUG_ASSIGN_ERROR(status, NC_ENULLCOUNT)
                    goto err_check;
                }
                for (i=0; i<varp->ndims; i++) {
                    if (counts[j][i] < 0) { /* no negative counts[][] */
                        DEBUG_ASSIGN_ERROR(status, NC_ENEGATIVECNT)
                        goto err_check;
                    }
                    bufcount_j *= counts[j][i];
                }
                bufcount += bufcount_j;
            }
        }
        /* assign buftype match with the variable's data type */
        buftype = ncmpio_nc2mpitype(varp->type);
    }

    cbuf = buf;
    if (bufcount > 0) { /* flexible API is used */
        /* pack buf into cbuf, a contiguous buffer */
        int isderived, iscontig_of_ptypes;
        MPI_Offset bnelems=0;

        /* ptype (primitive MPI data type) from buftype
         * el_size is the element size of ptype
         * bnelems is the total number of ptype elements in buftype
         */
        status = ncmpio_dtype_decode(buftype, &ptype, &el_size, &bnelems,
                                     &isderived, &iscontig_of_ptypes);

        if (status != NC_NOERR) goto err_check;

        if (bufcount != (int)bufcount) {
            DEBUG_ASSIGN_ERROR(status, NC_EINTOVERFLOW)
            goto err_check;
        }

        /* check if buftype is contiguous, if not, pack to one, cbuf */
        if (! iscontig_of_ptypes && bnelems > 0) {
            position = 0;
            packsize  = bnelems*el_size;
            if (packsize != (int)packsize) {
                DEBUG_ASSIGN_ERROR(status, NC_EINTOVERFLOW)
                goto err_check;
            }
            cbuf = NCI_Malloc((size_t)packsize);
            free_cbuf = 1;
            MPI_Pack(buf, (int)bufcount, buftype, cbuf, (int)packsize,
                         &position, MPI_COMM_SELF);
        }
    }
    else {
        /* this subroutine is called from a high-level API */
        status = NCMPII_ECHAR(varp->type, buftype);
        if (status != NC_NOERR) goto err_check;

        ptype = buftype;
        el_size = ncmpio_xlen_nc_type(varp->type);
    }

    /* We allow counts == NULL and treat this the same as all 1s */
    if (counts == NULL) {
        _counts    = (MPI_Offset**) NCI_Malloc((size_t)num *
                                               sizeof(MPI_Offset*));
        _counts[0] = (MPI_Offset*)  NCI_Malloc((size_t)num * varp->ndims *
                                               SIZEOF_MPI_OFFSET);
        for (i=1; i<num; i++)
            _counts[i] = _counts[i-1] + varp->ndims;
        for (i=0; i<num; i++)
            for (j=0; j<varp->ndims; j++)
                _counts[i][j] = 1;
    }
    else
        _counts = (MPI_Offset**) counts;

    /* break buf into num pieces */
    bufp = (char*)cbuf;
    for (i=0; i<num; i++) {
        MPI_Offset buflen;
        for (buflen=1, j=0; j<varp->ndims; j++) {
            if (_counts[i][j] < 0) { /* any negative counts[][] is illegal */
                DEBUG_ASSIGN_ERROR(status, NC_ENEGATIVECNT)
                goto err_check;
            }
            buflen *= _counts[i][j];
        }
        if (buflen == 0) continue;
        status = log_put_var(ncp, varp->varid, starts[i], _counts[i], NULL,
                                     NULL, bufp, buflen, ptype, API_VARA, itype);
        /* req_id is reused because requests in a varn is considered in the
         * same group */
        if (status != NC_NOERR) goto err_check;

        /* use isSamegroup so we end up with one nonblocking request (only the
         * first request gets a request ID back, the rest reuse the same ID.
         * This single ID represents num nonblocking requests */
        bufp += buflen * el_size;
    }

err_check:
    if (_counts != NULL && _counts != counts) {
        NCI_Free(_counts[0]);
        NCI_Free(_counts);
    }
    
    if (status != NC_NOERR) {
        if (free_cbuf) NCI_Free(cbuf);
        return status;
    }

    num = 1;
    if (status != NC_NOERR)
        /* This can only be reached for COLL_IO and safe_mode == 0.
           Set num=0 just so this process can participate the collective
           calls in wait_all */
        num = 0;


    /* unpack to user buf, if buftype is noncontiguous */
    if (status == NC_NOERR && free_cbuf) {
        position = 0;
        MPI_Unpack(cbuf, (int)packsize, &position, buf, (int)bufcount, buftype,
                   MPI_COMM_SELF);
    }

    /* return the first error, if there is one */
    if (status == NC_NOERR) status = err;
    if (status == NC_NOERR) status = st;

    if (free_cbuf) NCI_Free(cbuf);

    return status;
}

/*
 * Prepare a single log entry to be write to log
 * IN    nclogp:    log structure to log this entry
 * IN    varp:    NC_var structure associate to this entry
 * IN    start: start in put_var* call
 * IN    count: count in put_var* call
 * IN    stride: stride in put_var* call
 * IN    bur:    buffer of data to write
 * IN    buftype:    buftype as in ncmpio_getput_varm, MPI_PACKED indicate a flexible api
 * IN    PackedSize:    Size of buf in byte, only valid when buftype is MPI_PACKED
 */
int log_put(NC_Log *nclogp, NC_var *varp, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Datatype buftype, int PackedSize){
    int err, varid, dim;
    int itype;    /* Type used in log file */
    char *buffer;
    MPI_Offset esize, dataoff;
    MPI_Offset *Start, *Count, *Stride;
    ssize_t ioret;
    NC_Log_metadataentry *entryp;
    NC_Log_metadataheader *headerp;

    /* Enddef must be called at least once */
    if (nclogp->metalog_fd < 0){
        DEBUG_RETURN_ERROR(NC_ELOGNOTINIT);        
    }

    /* Get variable id and dimension */
    dim = varp->ndims;
    varid = varp->varid;

    /* Convert to log types 
     * Log spec has different enum of types than MPI
     */
    switch (buftype) {
    case MPI_CHAR:    /* put_*_text */
        itype = NC_LOG_TYPE_TEXT;
        break;
    case MPI_SIGNED_CHAR:    /* put_*_schar */
        itype = NC_LOG_TYPE_SCHAR;
        break;
    case MPI_UNSIGNED_CHAR:    /* put_*_uchar */
        itype = NC_LOG_TYPE_UCHAR;
        break;
    case MPI_UNSIGNED_SHORT: /* put_*_ushort */
        itype = NC_LOG_TYPE_USHORT;
        break;
    case MPI_INT: /* put_*_int */
        itype = NC_LOG_TYPE_INT;
        break;
    case MPI_UNSIGNED: /* put_*_uint */
        itype = NC_LOG_TYPE_UINT;
        break;
    case MPI_FLOAT: /* put_*_float */
        itype = NC_LOG_TYPE_FLOAT;
        break;
    case MPI_DOUBLE: /* put_*_double */
        itype = NC_LOG_TYPE_DOUBLE;
        break;
    case MPI_LONG_LONG_INT: /* put_*_longlong */
        itype = NC_LOG_TYPE_LONGLONG;
        break;
    case MPI_UNSIGNED_LONG_LONG: /* put_*_ulonglong */
        itype = NC_LOG_TYPE_ULONGLONG;
        break;
    default: /* Unrecognized type */
        DEBUG_RETURN_ERROR(NC_EINVAL);
        break;
    }
    
    /* Writing to data log
     * Note: Metadata record indicate completion, so data must go first 
     */
    
    /* Find out the location of data in datalog
     * Which is current possition in data log 
     * Datalog descriptor should always points to the end of file
     * Position must be recorded first before writing
     */
    dataoff = (MPI_Offset)nclogp->datalogsize;
    
    /* 
     * Write data log
     * We only increase datalogsize by amount actually write
     */
    ioret = write(nclogp->datalog_fd, buf, PackedSize);
    if (ioret < 0){
        err = nclogio_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    nclogp->datalogsize += ioret;
    if (ioret != PackedSize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }
 
    /* Prepare metadata entry header */
    
    /* Record offset 
     * Note: metadata will be updated after allocating metadata buffer space, seek must be done first 
     */
    
    /* Seek to the head of metadata
     * Note: EOF may not be the place for next entry after a flush
     * Note: metadata size will be updated after allocating metadata buffer space, seek must be done first 
     */
    ioret = lseek(nclogp->metalog_fd, nclogp->metadata.nused, SEEK_SET);   
    if (ioret < 0){
        DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
    }

    /* Size of metadata entry
     * Include metadata entry header and variable size additional data (start, count, stride) 
     */
    esize = sizeof(NC_Log_metadataentry) + dim * 3 * SIZEOF_MPI_OFFSET;
    
    /* Allocate space for metadata entry header */
    buffer = (char*)nclogio_log_buffer_alloc(&nclogp->metadata, esize);
    entryp = (NC_Log_metadataentry*)buffer;
    entryp->esize = esize; /* Entry size */
    entryp->itype = itype; /* Variable type */
    entryp->varid = varid;  /* Variable id */
    entryp->ndims = dim;  /* Number of dimensions of the variable*/
	entryp->data_len = PackedSize; /* The size of data in bytes. The size that will be write to data log */

    /* Find out the location of data in datalog
     * Which is current possition in data log 
     * Datalog descriptor should always points to the end of file
     */
    ioret = lseek(nclogp->datalog_fd, 0, SEEK_CUR);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
    }
    entryp->data_off = dataoff;
    
    
    /* Determine the api kind of original call
     * If stride is NULL, we log it as a vara call, otherwise, a vars call 
     * Upper layer translates var1 and var to vara, so we only have vara and vars
     */
    if (stride == NULL){
        entryp->api_kind = NC_LOG_API_KIND_VARA;
    }
    else{
        entryp->api_kind = NC_LOG_API_KIND_VARS;
    }
    
    /* Calculate location of start, count, stride in metadata buffer */
    Start = (MPI_Offset*)(buffer + sizeof(NC_Log_metadataentry));
    Count = Start + dim;
    Stride = Count + dim;
    
    /* Fill up start, count, and stride */
    memcpy(Start, start, dim * SIZEOF_MPI_OFFSET);
    memcpy(Count, count, dim * SIZEOF_MPI_OFFSET);
    if(stride != NULL){
        memcpy(Stride, stride, dim * SIZEOF_MPI_OFFSET);
    }
    /* Write meta data log */
    ioret = write(nclogp->metalog_fd, buffer, esize);
    if (ioret < 0){
        err = nclogio_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != esize){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Increase number of entry
     * This must be the final step of a log record
     * Increasing num_entries marks the completion of the record
     */

    /* Increase num_entries in the metadata buffer */
    headerp = (NC_Log_metadataheader*)nclogp->metadata.buffer;
    headerp->num_entries++;
    /* Seek to the location of num_entries
     * Note: location need to be updated when struct change
     */
    ioret = lseek(nclogp->metalog_fd, sizeof(NC_Log_metadataheader) - sizeof(headerp->basename) - sizeof(headerp->basenamelen) - sizeof(headerp->num_entries), SEEK_SET);
    if (ioret < 0){
        DEBUG_RETURN_ERROR(nclogio_handle_io_error("lseek"));
    }
    /* Overwrite num_entries
     * This marks the completion of the record
     */
    ioret = write(nclogp->metalog_fd, &headerp->num_entries, SIZEOF_MPI_OFFSET);
    if (ioret < 0){
        err = nclogio_handle_io_error("write");
        if (err == NC_EFILE){
            err = NC_EWRITE;
        }
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != SIZEOF_MPI_OFFSET){
        DEBUG_RETURN_ERROR(NC_EWRITE);
    }

    /* Record data size */
    nclogio_log_sizearray_append(&nclogp->entrydatasize, entryp->data_len);
    
    return NC_NOERR;
}

