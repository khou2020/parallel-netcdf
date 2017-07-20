/*
 *  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
 *  See COPYRIGHT notice in top-level directory.
 */
/* $Id$ */

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdlib.h>

#include <pnetcdf.h>
#include <dispatch.h>

/*----< ncmpi_inq_att() >----------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_att(int         ncid,
              int         varid,
              const char *name, /* input, attribute name */
              nc_type    *xtypep,
              MPI_Offset *lenp)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_att() */
    return pncp->driver->inq_att(pncp->ncp, varid, name, xtypep, lenp);
}

/*----< ncmpi_inq_atttype() >------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_atttype(int         ncid,
                  int         varid,
                  const char *name, /* input, attribute name */
                  nc_type    *xtypep)
{
    return ncmpi_inq_att(ncid, varid, name, xtypep, NULL);
}

/*----< ncmpi_inq_attlen() >-------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_attlen(int         ncid,
                 int         varid,
                 const char *name, /* input, attribute name */
                 MPI_Offset *lenp)
{
    return ncmpi_inq_att(ncid, varid, name, NULL, lenp);
}

/*----< ncmpi_inq_attid() >--------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_attid(int         ncid,
                int         varid,
                const char *name,
                int        *attnump)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_attid() */
    return pncp->driver->inq_attid(pncp->ncp, varid, name, attnump);
}

/*----< ncmpi_inq_attname() >------------------------------------------------*/
/* This is an independent subroutine. */
int
ncmpi_inq_attname(int   ncid,
                  int   varid,
                  int   attnum,
                  char *name) /* output, attribute name */
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_inq_attname() */
    return pncp->driver->inq_attname(pncp->ncp, varid, attnum, name);
}

/*----< ncmpi_copy_att() >---------------------------------------------------*/
/* This is a collective subroutine.
 * ncid_out must be in define mode. If varid_in's attribute name has alreay
 * existed in varid_out, it means to overwrite the attribute in varid_out.
 * In this case, if the space used by varid_in's attribute is larger than
 * varid_out's, then this API must be called when the file is in define mode.
 */
int
ncmpi_copy_att(int         ncid_in,
               int         varid_in,
               const char *name,
               int         ncid_out,
               int         varid_out)
{
    int err;
    PNC *pncp_in, *pncp_out;

    /* check if ncid_in is valid */
    err = PNC_check_id(ncid_in, &pncp_in);
    if (err != NC_NOERR) return err;

    /* check if ncid_out is valid */
    err = PNC_check_id(ncid_out, &pncp_out);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_copy_att() */
    return pncp_in->driver->copy_att(pncp_in->ncp,  varid_in, name,
                                     pncp_out->ncp, varid_out);
}

/*----< ncmpi_rename_att() >-------------------------------------------------*/
/* This is a collective subroutine. If the new name is longer than the old
 * name, this API must be called in define mode.
 */
int
ncmpi_rename_att(int         ncid,
                 int         varid,
                 const char *name,
                 const char *newname)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_rename_att() */
    return pncp->driver->rename_att(pncp->ncp, varid, name, newname);
}

/*----< ncmpi_del_att() >----------------------------------------------------*/
/* This is a collective subroutine.
 * This API must be called in define mode.
 */
int
ncmpi_del_att(int         ncid,
              int         varid,
              const char *name)
{
    int err;
    PNC *pncp;

    /* check if ncid is valid */
    err = PNC_check_id(ncid, &pncp);
    if (err != NC_NOERR) return err;

    /* calling the subroutine that implements ncmpi_del_att() */
    return pncp->driver->del_att(pncp->ncp, varid, name);
}

