/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

/*
 * This file implements the following PnetCDF APIs.
 *
 * ncmpi_def_dim()    : dispatcher->def_dim()
 * ncmpi_inq_dimid()  : dispatcher->inq_dimid()
 * ncmpi_inq_dim()    : dispatcher->inq_dim()
 * ncmpi_rename_dim() : dispatcher->rename_dim()
 */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include <pnc_debug.h>
#include <common.h>
#include <nclogio_driver.h>

int
nclogio_def_dim(void       *ncdp,
              const char *name,
              MPI_Offset  size,
              int        *dimidp)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->def_dim(nclogp->ncp, name, size, dimidp);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_inq_dimid(void       *ncdp,
                const char *name,
                int        *dimid)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->inq_dimid(nclogp->ncp, name, dimid);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_inq_dim(void       *ncdp,
              int         dimid,
              char       *name,
              MPI_Offset *sizep)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->inq_dim(nclogp->ncp, dimid, name, sizep);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}

int
nclogio_rename_dim(void       *ncdp,
                 int         dimid,
                 const char *newname)
{
    int err;
    NC_Log *nclogp = (NC_Log*)ncdp;
    
    err = nclogp->ncmpio_driver->rename_dim(nclogp->ncp, dimid, newname);
    if (err != NC_NOERR) return err;

    return NC_NOERR;
}
