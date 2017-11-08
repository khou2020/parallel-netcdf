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
#include <pnetcdf.h>

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
    NC_bb_file *f;

    /* Allocate buffer */
    f = (NC_bb_file*)NCI_Malloc(sizeof(NC_bb_file));
    f->buf = NCI_Malloc(BUFSIZE);
    if (f->buf == NULL){
        DEBUG_RETURN_ERROR(NC_ENOMEM);
    }
    // TODO: Adjustable bsize
    f->bsize = BUFSIZE;
    f->pos = 0;
    f->fpos = 0;
    f->bused = 0;
    MPI_Comm_rank(comm, &(f->rank));
    MPI_Comm_size(comm, &(f->np));

    /* Open file */
    if (f->rank == 0){
        f->fd = open(path, flag, 0744);
        if (f->fd < 0){
            err = ncmpii_error_posix2nc("open");
            free(f->buf); // Free the buffer if error occurs
            DEBUG_RETURN_ERROR(err);
        }
    }

    if (f->np > 1){
        MPI_Barrier(comm);
    }

    if (f->rank > 0){
        if (flag & O_CREAT){
            flag ^= O_CREAT;
        }
        f->fd = open(path, flag, 0744);
        if (f->fd < 0){
            err = ncmpii_error_posix2nc("open");
            free(f->buf); // Free the buffer if error occurs
            DEBUG_RETURN_ERROR(err);
        }
    }

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

    /* Close file */
    err = close(f->fd);
    if (err != 0){
        err = ncmpii_error_posix2nc("close");
        DEBUG_RETURN_ERROR(err);
    }

    NCI_Free(f);
    return NC_NOERR;
}

int ncbbio_file_write_core(NC_bb_file *f, void *buf, size_t count){
    int i;
    size_t sblock, soff, eblock, eoff;
    size_t off, len;
    size_t ioret;

    if (f->np > 0){
        sblock = f->fpos / BLOCKSIZE;
        soff =  f->fpos % BLOCKSIZE;
        eblock = (f->fpos + count) / BLOCKSIZE;
        eoff =  (f->fpos + count) % BLOCKSIZE;

        for(i = sblock; i <=eblock; i++){
            if (i == sblock){
                off = (i * f->np + f->rank) * BLOCKSIZE + soff;
                len = BLOCKSIZE - soff;
                if (len > count){
                    len = count;
                }
            }
            else if (i == eblock) {
                off = (i * f->np + f->rank) * BLOCKSIZE;
                len = eoff;
            }
            else{
                off = (i * f->np + f->rank) * BLOCKSIZE;
                len = BLOCKSIZE;
            }
            ioret = pwrite(f->fd, buf, len, off);
            buf += ioret;
        }
    }
    else{
        ioret = write(f->fd, buf, count);
    }

    f->fpos += count;

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
        ioret = ncbbio_file_write_core(f, f->buf, f->bused);
        /*
        if (ioret < 0){
            err = ncmpii_error_posix2nc("write");
            if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != f->bused){
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
        */
    }
    f->bused = 0;
    return NC_NOERR;
}

/*
 * Read buffered file
 * IN       f:    File structure
 * OUT    buf:    Buffer for read data
 * IN   count:    Size of buffer
 */
int ncbbio_file_read(NC_bb_file *f, void *buf, size_t count) {
    int i;
    size_t sblock, soff, eblock, eoff;
    size_t off, len;
    size_t ioret;

    if (f->np > 0){
        sblock = f->fpos / BLOCKSIZE;
        soff =  f->fpos % BLOCKSIZE;
        eblock = (f->fpos + count) / BLOCKSIZE;
        eoff =  (f->fpos + count) % BLOCKSIZE;

        for(i = sblock; i <=eblock; i++){
            if (i == sblock){
                off = (i * f->np + f->rank) * BLOCKSIZE + soff;
                len = BLOCKSIZE - soff;
                if (len > count){
                    len = count;
                }
            }
            else if (i == eblock) {
                off = (i * f->np + f->rank) * BLOCKSIZE;
                len = eoff;
            }
            else{
                off = (i * f->np + f->rank) * BLOCKSIZE;
                len = BLOCKSIZE;
            }
            ioret = pread(f->fd, buf, len, off);
            buf += ioret;
        }
    }
    else{
        ioret = read(f->fd, buf, count);
    }

    f->pos += count;
    f->fpos += count;

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
        ioret = ncbbio_file_write_core(f, buf + astart, aend - astart); 
        /*
        if (ioret < 0){
            err = ncmpii_error_posix2nc("write");
            if (err == NC_EFILE) DEBUG_ASSIGN_ERROR(err, NC_EWRITE);
            DEBUG_RETURN_ERROR(err);
        }
        if (ioret != aend - astart){
            DEBUG_RETURN_ERROR(NC_EWRITE);
        }
        */
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

    if (f->np > 0){
        // Update position
        if (whence == SEEK_SET){ 
            f->fpos = off;
            
        }
        else if (whence == SEEK_CUR){
            f->fpos += off;
        }
        else{
            DEBUG_RETURN_ERROR(NC_ENOTSUPPORT); 
        }
    }
    else{
        ioret = lseek(f->fd, off, whence);
        if (ioret < 0){
            err = ncmpii_error_posix2nc("lseek");
            DEBUG_RETURN_ERROR(err); 
        }
            
        // Update position
        if (whence == SEEK_SET){ 
            f->fpos = off;
        }
        else if (whence == SEEK_CUR){
            f->fpos += off;
        }
        else{
            DEBUG_RETURN_ERROR(NC_ENOTSUPPORT); 
        }
    }
    f->pos = f->fpos;

    return NC_NOERR;
}

