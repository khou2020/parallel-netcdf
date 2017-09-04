/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <dispatch.h>
#include <nclogio_driver.h>

static PNC_driver nclogio_driver = {
    /* FILE APIs */
    nclogio_create,
    nclogio_open,
    nclogio_close,
    nclogio_enddef,
    nclogio__enddef,
    nclogio_redef,
    nclogio_sync,
    nclogio_abort,
    nclogio_set_fill,
    nclogio_inq,
    nclogio_inq_misc,
    nclogio_sync_numrecs,
    nclogio_begin_indep_data,
    nclogio_end_indep_data,

    /* DIMENSION APIs */
    nclogio_def_dim,
    nclogio_inq_dimid,
    nclogio_inq_dim,
    nclogio_rename_dim,

    /* ATTRIBUTE APIs */
    nclogio_inq_att,
    nclogio_inq_attid,
    nclogio_inq_attname,
    nclogio_copy_att,
    nclogio_rename_att,
    nclogio_del_att,
    nclogio_get_att,
    nclogio_put_att,

    /* VARIABLE APIs */
    nclogio_def_var,
    nclogio_def_var_fill,
    nclogio_fill_var_rec,
    nclogio_inq_var,
    nclogio_inq_varid,
    nclogio_rename_var,
    nclogio_get_var,
    nclogio_put_var,
    nclogio_get_varn,
    nclogio_put_varn,
    nclogio_get_vard,
    nclogio_put_vard,
    nclogio_iget_var,
    nclogio_iput_var,
    nclogio_bput_var,
    nclogio_iget_varn,
    nclogio_iput_varn,
    nclogio_bput_varn,

    nclogio_buffer_attach,
    nclogio_buffer_detach,
    nclogio_wait,
    nclogio_cancel
};

PNC_driver* nclogio_inq_driver(void) {
    return &nclogio_driver;
}

