#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <common.h>
#include <pnc_debug.h>
#include <common.h>
#include <pnetcdf.h>
#include <ncbbio_driver.h>
#include <fcntl.h>

#define BUFSIZE 8388608
#define BLOCKSIZE 8388608

/*
 * Open buffered file
 * IN      comm:    Communicator for shared file
 * IN      path:    Path of file
 * IN      flag:    File open flag
 * OUT       fd:    File structure
 */
int ncbbio_file_open(MPI_Comm comm, char *path, int flag, NC_bb_file **fd) {
    int err;
#ifdef NC_BB_SHARED_LOG
    int rank, np;
    int amode = 0;
    MPI_Datatype ftype;
    MPI_Datatype btype;
#endif    
    NC_bb_file *f;

    /* Allocate buffer */
    f = (NC_bb_file*)NCI_Malloc(sizeof(NC_bb_file));
    f->buf = NCI_Malloc(BUFSIZE);
    if (f->buf == NULL){
        DEBUG_RETURN_ERROR(NC_NOMEM);
    }
    // TODO: Adjustable bsize
    f->bsize = BUFSIZE;
    f->pos = 0;
    f->bused = 0;

#ifdef NC_BB_SHARED_LOG
    /* Translate posix flag to MPI flag */
    if (flag & O_RDWR) {
        amode |= MPI_MODE_RDWR;
    }
    if (flag & O_CREAT) {
        amode |= MPI_MODE_CREATE;
    }
    if (flag & O_EXCL) {
        amode |= MPI_MODE_EXCL;
    }
    
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &np);
    /* Open file */
    err = MPI_File_open(comm, path, amode, MPI_INFO_NULL, &(f->fd));
    if (err != MPI_SUCCESS){
        err = ncmpii_error_mpi2nc(err, "open");
        free(f->buf); // Free the buffer if error occurs
        DEBUG_RETURN_ERROR(err);
    }
    /* Setting file view for shared file */
    MPI_Type_contiguous(BLOCKSIZE, MPI_BYTE, &btype);
    MPI_Type_commit(&btype);    // Stride
    MPI_Type_create_resized(btype, 0, BLOCKSIZE * np, &ftype);
    MPI_Type_commit(&ftype);    // Stride of all processes
    err = MPI_File_set_view(f->fd, BLOCKSIZE * rank, MPI_BYTE, ftype, "native",
                            MPI_INFO_NULL);
    if (err != MPI_SUCCESS){
        err = ncmpii_error_mpi2nc(err, "setview");
        free(f->buf);
        DEBUG_RETURN_ERROR(err);
    }
#else
    /* Open file */
    f->fd = open(path, flag, 0744);
    if (f->fd < 0){
        err = ncmpii_error_posix2nc("open");
        free(f->buf); // Free the buffer if error occurs
        DEBUG_RETURN_ERROR(err);
    }
#endif
    *fd = f;
    return NC_NOERR;
}

/*
 * Close buffered file
 * IN       f:    File structure
 */
int ncbbio_file_close(NC_bb_file *f) {
    int err;

    /* Free the buffer */
    NCI_Free(f->buf);

#ifdef NC_BB_SHARED_LOG
    /* Close file */
    err = MPI_File_close(&(f->fd));
    if (err != MPI_SUCCESS){
        err = ncmpii_error_mpi2nc(err, "close");
        DEBUG_RETURN_ERROR(err);
    }
#else
    /* Close file */
    err = close(f->fd);
    if (err != 0){
        err = ncmpii_error_posix2nc("close");
        DEBUG_RETURN_ERROR(err);
    }
#endif

    NCI_Free(f);
    return NC_NOERR;
}

/*
 * Flush file buffer
 * IN       f:    File structure
 */
int ncbbio_file_flush(NC_bb_file *f){
    int err;
    ssize_t ioret;

    /* Write data if buffer is not empty */
    if (f->bused > 0){
#ifdef NC_BB_SHARED_LOG
        MPI_Status stat;
        err = MPI_File_write(f->fd, f->buf, f->bused, MPI_BYTE, &stat);
        if (err != MPI_SUCCESS){
            err = ncmpii_error_mpi2nc(err, "write");
            if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
            DEBUG_RETURN_ERROR(err);
        }
#else
        ioret = write(f->fd, f->buf, f->bused);
        if (ioret < 0){
            err = ncmpii_error_posix2nc("write");
            if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != f->bused){
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
#endif
    }
    f->bused = 0;
}

/*
 * Read buffered file
 * IN       f:    File structure
 * OUT    buf:    Buffer for read data
 * IN   count:    Size of buffer
 */
int ncbbio_file_read(NC_bb_file *f, void *buf, size_t count) {
    int err;
    ssize_t ioret;

#ifdef NC_BB_SHARED_LOG
    MPI_Status stat;       
    err = MPI_File_read(f->fd, buf, count, MPI_BYTE, &stat);
    if (err != MPI_SUCCESS){
        err = ncmpii_error_mpi2nc(err, "write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
        DEBUG_RETURN_ERROR(err);
    }
#else
    ioret = read(f->fd, buf, count);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("write");
        if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EREAD);
        DEBUG_RETURN_ERROR(err);
    }
    if (ioret != f->bused){
        DEBUG_RETURN_ERROR(NC_EREAD);
    }
#endif
    f->pos += count;
    return NC_NOERR;
}

/*
 * Write buffered file
 * IN       f:    File structure
 * IN     buf:    Data buffer to write
 * IN   count:    Size of buffer
 */
int ncbbio_file_write(NC_bb_file *f, void *buf, size_t count) {
    int err;
    ssize_t ioret;
    size_t wsize;
    size_t astart, aend;
    char *cbuf = buf;

    // Calculate the first alligned position with the write region
    astart = (f->bsize - f->pos % f->bsize) % f->bsize;
    if (astart > count) {
        astart = 0;
    }
    // Calculate the last alligned position with the write region
    aend = f->pos + count - (f->pos + count) % f->bsize;
    if (aend < f->pos){
        aend = 0;
    }
    else{
        aend -= f->pos;
    }

    /*
     * If there are data in the buffer, we must at a unaligned position
     * Combine data before astart with data in the buffer
     */
    if (f->bused > 0){
        memcpy(f->buf + f->bused, cbuf, astart); 
        f->bused += astart;
        // Flush the buffer if it is full
        if (f->bused == f->bsize){
            err = ncbbio_file_flush(f);
            if (err != NC_NOERR){
                return err;
            }
        }
    }
    else{
        astart = 0;
    }
    
    /*
     * Write aligned section as usual
     * From astart to aend
     */
    if (aend > astart) {
#ifdef NC_BB_SHARED_LOG
        MPI_Status stat;       
        err = MPI_File_write(f->fd, buf + astart, aend - astart, MPI_BYTE,
                             &stat);
        if (err != MPI_SUCCESS){
            err = ncmpii_error_mpi2nc(err, "write");
            if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
            DEBUG_RETURN_ERROR(err);
        }
#else
        ioret = write(f->fd, buf + astart, aend - astart); 
        if (ioret < 0){
            err = ncmpii_error_posix2nc("write");
            if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != f->bused){
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
#endif
    }

    /*
     * Place the final section to the buffer
     * After aend
     */
    if (count > aend){
        memcpy(f->buf + f->bused, cbuf + aend, count - aend);
        f->bused += count - aend;
    }
    
    f->pos += count;

    return NC_NOERR;
}

/*
 * Seek buffered file
 * IN       f:    File structure
 * IN     off:    Offset to seek
 * IN  whence:    Type of offset
 */
int ncbbio_file_seek(NC_bb_file *f, size_t off, int whence) {
    int err;
    off_t ioret;

    /* 
     * Flush the buffer
     * We assume buffered data region starts immediately after cursor position
     * When we change the cursor possition, we need to flush the buffer
     */
    err = ncbbio_file_flush(f);
    if (err != NC_NOERR){
        return err;
    }
#ifdef NC_BB_SHARED_LOG
    // Translate posix whence to MPI whence
    int mpiwhence;
    if (whence == SEEK_SET){ 
        mpiwhence = MPI_SEEK_SET;
    }
    else if (whence == SEEK_CUR){
        mpiwhence = MPI_SEEK_CUR;
    }
    else{
        DEBUG_RETURN_ERROR(NC_ENOTSUPPORT); 
    }
    err = MPI_File_seek(f->fd, off, mpiwhence);
    if (err != MPI_SUCCESS){
        err = ncmpii_error_mpi2nc(err, "seek");
        DEBUG_RETURN_ERROR(err);
    }
#else
    ioret = lseek(f->fd, off, whence);
    if (ioret < 0){
        err = ncmpii_error_posix2nc("lseek");
        DEBUG_RETURN_ERROR(err); 
    }
#endif
    // Update position
    if (whence == SEEK_SET){ 
        f->pos = off;
    }
    else if (whence == SEEK_CUR){
        f->pos += off;
    }
    else{
        DEBUG_RETURN_ERROR(NC_ENOTSUPPORT); 
    }
    return NC_NOERR;
}

