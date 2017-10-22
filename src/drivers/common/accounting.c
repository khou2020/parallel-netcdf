/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <pnetcdf.h>
#include <pnc_debug.h>
#include <common.h>

double ncmpii_log_flush_replay_time;
double ncmpii_log_flush_data_rd_time;
double ncmpii_log_flush_put_time;
double ncmpii_log_flush_wait_time;
double ncmpii_log_flush_time;
double ncmpii_log_put_data_wr_time;
double ncmpii_log_put_meta_wr_time;
double ncmpii_log_put_num_wr_time;
double ncmpii_log_put_time;
double ncmpii_log_create_time;
double ncmpii_log_enddef_time;
double ncmpii_log_close_time;
double ncmpii_log_total_time;
MPI_Offset ncmpii_log_total_data;
MPI_Offset ncmpii_log_total_meta;
MPI_Offset ncmpii_log_max_buffer;

static double log_flush_replay_time;
static double log_flush_data_rd_time;
static double log_flush_put_time;
static double log_flush_wait_time;
static double log_flush_time;
static double log_put_data_wr_time;
static double log_put_meta_wr_time;
static double log_put_num_wr_time;
static double log_put_time;
static double log_create_time;
static double log_enddef_time;
static double log_close_time;
static double log_total_time;
static MPI_Offset log_total_data;
static MPI_Offset log_total_meta;
static MPI_Offset log_max_buffer;

void ncmpii_reset_bb_counter(){
    ncmpii_log_flush_replay_time = 0;
    ncmpii_log_flush_data_rd_time = 0;
    ncmpii_log_flush_put_time = 0;
    ncmpii_log_flush_wait_time = 0;
    ncmpii_log_flush_time = 0;
    ncmpii_log_put_data_wr_time = 0;
    ncmpii_log_put_meta_wr_time = 0;
    ncmpii_log_put_num_wr_time = 0;
    ncmpii_log_put_time = 0;
    ncmpii_log_create_time = 0;
    ncmpii_log_enddef_time = 0;
    ncmpii_log_close_time = 0;
    ncmpii_log_total_time = 0;
    ncmpii_log_total_data = 0;
    ncmpii_log_total_meta = 0;
    ncmpii_log_max_buffer = 0; 
}

void ncmpii_update_bb_counter(){
    log_flush_replay_time += ncmpii_log_flush_replay_time;
    log_flush_data_rd_time += ncmpii_log_flush_data_rd_time;
    log_flush_put_time += ncmpii_log_flush_put_time;
    log_flush_wait_time += ncmpii_log_flush_wait_time;
    log_flush_time += ncmpii_log_flush_time;
    log_put_data_wr_time += ncmpii_log_put_data_wr_time;
    log_put_meta_wr_time += ncmpii_log_put_meta_wr_time;
    log_put_num_wr_time += ncmpii_log_put_num_wr_time;
    log_put_time += ncmpii_log_put_time;
    log_create_time += ncmpii_log_create_time;
    log_enddef_time += ncmpii_log_enddef_time;
    log_close_time += ncmpii_log_close_time;
    log_total_time += ncmpii_log_total_time;
    log_total_data += ncmpii_log_total_data;
    log_total_meta += ncmpii_log_total_meta;
    if (log_max_buffer < ncmpii_log_max_buffer) {
        log_max_buffer = ncmpii_log_max_buffer; 
    }
}

/*----< ncmpi_inq_bb_time() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_bb_time(double *total_time, double *create_time, double *enddef_time, double *put_time, double *flush_time, double *close_time)
{
    // Fill time info
    if (total_time != NULL){
        *total_time = log_total_time;
    }
    if (create_time != NULL){
        *create_time = log_create_time;
    }
    if (enddef_time != NULL){
        *enddef_time = log_enddef_time;
    }
    if (put_time != NULL){
        *put_time = log_put_time;
    }
    if (flush_time != NULL){
        *flush_time = log_flush_time;
    }
    if (close_time != NULL){
        *close_time = log_close_time;
    }

    return NC_NOERR;
}

/*----< ncmpi_inq_bb_time_put() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_bb_time_put(double *put_data_wr_time, double *put_meta_wr_time, double *put_num_wr_time)
{
    // Fill time info
    if (put_data_wr_time != NULL){
        *put_data_wr_time = log_put_data_wr_time;
    }
    if (put_meta_wr_time != NULL){
        *put_meta_wr_time = log_put_meta_wr_time;
    }
    if (put_num_wr_time != NULL){
        *put_num_wr_time = log_put_num_wr_time;
    }
    
    return NC_NOERR;
}

/*----< ncmpi_inq_bb_time_put() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_bb_time_flush(double *flush_replay_time, double *flush_data_rd_time, double *flush_put_time, double *flush_wait_time)
{
    // Fill time info
    if (flush_replay_time != NULL){
        *flush_replay_time = log_flush_replay_time;
    }
    if (flush_data_rd_time != NULL){
        *flush_data_rd_time = log_flush_data_rd_time;
    }
    if (flush_put_time != NULL){
        *flush_put_time = log_flush_put_time;
    }
    if (flush_wait_time != NULL){
        *flush_wait_time = log_flush_wait_time;
    }

    return NC_NOERR;
}

/*----< ncmpi_inq_buffer_size() >--------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_bb_size(MPI_Offset *datasize, MPI_Offset *metasize, MPI_Offset *buffersize)
{
    /* Fill size info */
    if (datasize != NULL){
        *datasize = log_total_data;
    }
    if (metasize != NULL){
        *metasize = log_total_meta;
    }
    if (buffersize != NULL){
        *buffersize = log_max_buffer;
    }

    return NC_NOERR;
}