#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pnetcdf.h>

int zip(char *in, char *out) {
    int i, j, err;
    int rank, np;
    int fin, fout;
    int varid, attid, dimid, cmode;
    int nvars, ndims, natts;
    int *dimids, maxndims;
    MPI_Offset attlen;
    MPI_Offset *dimlen;
    nc_type vtype;
    MPI_Datatype btype;
    size_t datasize, cdatasize;
    MPI_Offset varsize, esize;
    char vname[1024], name[1024];
    char *data;
    char *cdata;

    // Init
    dimlen = NULL;
    datasize = 0;
    data = NULL;
    cdata = NULL;
    maxndims = 1024;
    dimids = (int*)malloc(maxndims * sizeof(int));

    // Open file
    ncmpi_open(MPI_COMM_WORLD, in, NC_NOWRITE, MPI_INFO_NULL, &fin);

    // Create compressed file
    ncmpi_inq_version(fin, &cmode);
    ncmpi_create(MPI_COMM_WORLD, out, cmode, MPI_INFO_NULL, &fout);

    // Copy dimensions
    ncmpi_inq_ndims(fin, &ndims);
    dimlen = (MPI_Offset*)malloc(ndims * sizeof(MPI_Offset));
    for(i = 0; i < ndims; i++){
        ncmpi_inq_dim(fin, i, name, dimlen + i);
        ncmpi_def_dim(fout, name, dimlen[i], &dimid);
    }

    // Copy global attributes
    ncmpi_inq_natts(fin, &natts);
    for(i = 0; i < natts; i++){
        ncmpi_inq_attname (fin, NC_GLOBAL, i, name);
        ncmpi_copy_att(fin, NC_GLOBAL, name, fout, NC_GLOBAL);
    }
    
    // Copy variables
    ncmpi_inq_nvars(fin, &nvars);
    // Reserve space for compression attributes and dimensions
    ncmpi__enddef(fout, nvars * 1024, 0, 0, 0);
    for(varid = 0; varid < nvars; varid++){
        // Get variable properties
        ncmpi_inq_varndims(fin, varid, &ndims);
        if (maxndims < ndims){
            maxndims = ndims;
            dimids = (int*)realloc(dimids, maxndims * sizeof(int));
        }
        ncmpi_inq_var(fin, varid, vname, &vtype, &ndims, dimids, &natts);

        // Calculate data size
        varsize = 1;
        for(i = 0; i < ndims; i++){
            varsize *= dimlen[dimids[i]];
        }
        esize = NC_Type_size(vtype);
        varsize *= esize;
        // Prepare data buffer
        if (varsize > datasize){
            datasize = varsize;
            data = (char*)realloc(data, datasize);
        }
        // Read data
        btype = NC_Type_to_mpi_type(vtype);
        err = ncmpi_get_var_all(fin, varid, data, varsize / esize, btype);

        // Perform compression
        compress(data, varsize, &cdata, &cdatasize);
        
        // Create compressed variable
        ncmpi_redef(fout);
        sprintf(name, "_zip_%s", vname);
        ncmpi_def_dim(fout, name, cdatasize, &dimid);
        ncmpi_def_var(fout, vname, NC_BYTE, 1, &dimid, &i);

        // Copy variable attributes
        for(i = 0; i < natts; i++){
            ncmpi_inq_attname (fin, varid, i, name);
            ncmpi_copy_att(fin, varid, name, fout, varid);
        }

        // Record original dimension
        ncmpi_put_att(fout, varid, "_zip_originaldim", NC_INT, ndims, dimids);
        // Record variable type
        ncmpi_put_att(fout, varid, "_zip_originaltype", NC_INT, 1, &vtype);
        // Record set compression flag
        i = 1;
        ncmpi_put_att(fout, varid, "_zip_compressed", NC_INT, 1, &i);
        
        // Write comrpessed data
        ncmpi_enddef(fout);
        ncmpi_put_var_all(fout, varid, cdata, cdatasize, MPI_UNSIGNED_CHAR);

        // Free compressed data
        if (cdata != NULL){
            free(cdata);
        }
    }
    if (dimids != NULL){
        free(dimids);
    }
    if (dimlen != NULL){
        free(dimlen);
    }
    if (data != NULL){
        free(data);
    }

    ncmpi_close(fin);
    ncmpi_close(fout);

    return 0;
}