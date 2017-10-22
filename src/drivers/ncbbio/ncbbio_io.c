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

int ncbbio_file_open(MPI_Comm comm, char *path, int flag, NC_bb_file **fd) {
    int err;
#ifdef NC_BB_SHARED_LOG
    int rank, np;
    int amode = 0;
    MPI_Datatype ftype;
    MPI_Datatype btype;
#endif    
    NC_bb_file *f;

    f = (NC_bb_file*)NCI_Malloc(sizeof(NC_bb_file));
    f->buf = NCI_Malloc(BUFSIZE);
    f->bsize = BUFSIZE;
    f->pos = 0;
    f->bused = 0;
#ifdef NC_BB_SHARED_LOG
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
    err = MPI_File_open(comm, path, amode, MPI_INFO_NULL, &(f->fd));
    MPI_Error_string(err, estr, &elen);
    MPI_Type_contiguous(BLOCKSIZE, MPI_BYTE, &btype);
    MPI_Type_commit(&btype);
    MPI_Type_create_resized(btype, 0, BLOCKSIZE * np, &ftype);
    MPI_Type_commit(&ftype);
    MPI_File_set_view(f->fd, BLOCKSIZE * rank, MPI_BYTE, ftype, "native", MPI_INFO_NULL);
#else
    f->fd = open(path, flag, 0744);
#endif
    *fd = f;
    return NC_NOERR;
}

int ncbbio_file_close(NC_bb_file *f) {

    NCI_Free(f->buf);
#ifdef NC_BB_SHARED_LOG
    MPI_File_close(&(f->fd));
#else
    close(f->fd);
#endif
    NCI_Free(f);
    return NC_NOERR;
}

int ncbbio_file_flush(NC_bb_file *f){
    
    if (f->bused > 0){
#ifdef NC_BB_SHARED_LOG
        MPI_Status stat;
        MPI_File_write(f->fd, f->buf, f->bused, MPI_BYTE, &stat);
#else
        write(f->fd, f->buf, f->bused);
#endif
    }
    f->bused = 0;
}

int ncbbio_file_read(NC_bb_file *f, void *buf, size_t count) {
    int err;
#ifdef NC_BB_SHARED_LOG
    MPI_Status stat;       
    err = MPI_File_read(f->fd, buf, count, MPI_BYTE, &stat);
    
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
#ifdef NC_BB_SHARED_LOG
        MPI_Status stat;       
        MPI_File_write(f->fd, buf + astart, aend - astart, MPI_BYTE, &stat);
#else
        write(f->fd, buf + astart, aend - astart); 
#endif
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
#ifdef NC_BB_SHARED_LOG
    int mpiwhence;
    if (whence == SEEK_SET){ 
        mpiwhence = MPI_SEEK_SET;
    }
    else if (whence == SEEK_CUR){
        mpiwhence = MPI_SEEK_CUR;
    }
    else{
    }
    MPI_File_seek(f->fd, off, mpiwhence);
#else
    lseek(f->fd, off, whence);
#endif
    if (whence == SEEK_SET){ 
        f->pos = off;
    }
    else if (whence == SEEK_CUR){
        f->pos += off;
    }
    else{
        printf("ERROR\n");
    }
    return NC_NOERR;
}

