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

int ncbbio_file_open(char *path, int flag, NC_bb_file **fd) {
    NC_bb_file *f;

/*
 * if (ncbbp->datalog_fd < 0) {
 *          err = ncmpii_error_posix2nc("open");
 *          DEBUG_RETURN_ERROR(err);
 *     }
 * */

    f = (NC_bb_file*)NCI_Malloc(sizeof(NC_bb_file));
    f->buf = NCI_Malloc(BUFSIZE);
    f->bsize = BUFSIZE;
    f->pos = 0;
    f->bused = 0;
#ifdef NC_SHARED_LOG
    MPI_File_open(MPI_COMM_SELF, path, MPI_MODE_RDWR, MPI_INFO_NULL, &f->fd);
#else
    f->fd = open(path, flag, 0744);
#endif
    *fd = f;
    return NC_NOERR;
}

int ncbbio_file_close(NC_bb_file *f) {

    NCI_Free(f->buf);
#ifdef NC_SHARED_LOG
#else
    close(f->fd);
#endif
    NCI_Free(f);
    return NC_NOERR;
}

int ncbbio_file_flush(NC_bb_file *f){
    if (f->bused > 0){
#ifdef NC_SHARED_LOG
#else
        write(f->fd, f->buf, f->bused);
#endif
    }
    f->bused = 0;
}

int ncbbio_file_read(NC_bb_file *f, void *buf, size_t count) {
#ifdef NC_SHARED_LOG
#else
    read(f->fd, buf, count);
#endif
    f->pos += count;
    return NC_NOERR;
}

int ncbbio_file_write(NC_bb_file *f, void *buf, size_t count) {
    size_t wsize;
    size_t astart, aend;
    char *cbuf = buf;

    astart = (f->bsize - f->pos % f->bsize) % f->bsize;
    if (astart > count) {
        astart = 0;
    }
    aend = f->pos + count - (f->pos + count) % f->bsize;
    if (aend < f->pos){
        aend = 0;
    }
    else{
        aend -= f->pos;
    }

    if (f->bused > 0){
        memcpy(f->buf + f->bused, cbuf, astart); 
        f->bused += astart;
        if (f->bused == f->bsize){
            ncbbio_file_flush(f);
        }
    }
    else{
        astart = 0;
    }
    
    if (aend > astart) {
        write(f->fd, buf + astart, aend - astart); 
    }

    if (count > aend){
        memcpy(f->buf + f->bused, cbuf + aend, count - aend);
        f->bused += count - aend;
    }
    
    f->pos += count;

    return NC_NOERR;
}

int ncbbio_file_seek(NC_bb_file *f, size_t off, int whence) {
    ncbbio_file_flush(f);
#ifdef NC_SHARED_LOG
#else
    lseek(f->fd, off, whence);
#endif
    if (whence = SEEK_SET){ 
        f->pos = off;
    }
    else if (whence = SEEK_CUR){
        f->pos += off;
    }
    else if (whence = SEEK_END){
        f->pos = lseek(f->fd, 0, SEEK_CUR);
    }
    else{
    }
    return NC_NOERR;
}

