#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pnetcdf.h>

int unzip(char *in, char *out) {
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
    size_t datasize, ddatasize;
    MPI_Offset varsize, esize;
    char vname[1024], name[1024];
    char *data;
    char *ddata;

    // Init
    dimlen = NULL;
    datasize = 0;
    data = NULL;
    ddata = NULL;
    maxndims = 1024;
    dimids = (int*)malloc(maxndims * sizeof(int));

    // Open file
    ncmpi_open(MPI_COMM_WORLD, in, NC_NOWRITE, MPI_INFO_NULL, &fin);

    // Create decompressed file
    ncmpi_inq_version(fin, &cmode);
    ncmpi_create(MPI_COMM_WORLD, out, cmode, MPI_INFO_NULL, &fout);

    // Copy dimensions
    ncmpi_inq_ndims(fin, &ndims);
    dimlen = (MPI_Offset*)malloc(ndims * sizeof(MPI_Offset));
    for(i = 0; i < ndims; i++){
        ncmpi_inq_dim(fin, i, name, dimlen + i);
        if (strncmp(name, "_zip", 4) != 0) {
            ncmpi_def_dim(fout, name, dimlen[i], &dimid);
        }
    }

    // Copy global attributes
    ncmpi_inq_natts(fin, &natts);
    for(i = 0; i < natts; i++){
        ncmpi_inq_attname (fin, NC_GLOBAL, i, name);
        ncmpi_copy_att(fin, NC_GLOBAL, name, fout, NC_GLOBAL);
    }
    
    // Copy variables
    ncmpi_inq_nvars(fin, &nvars);
    // Reserve space for decompression attributes and dimensions
    ncmpi__enddef(fout, nvars * 1024, 0, 0, 0);
    for(varid = 0; varid < nvars; varid++){
        // Get variable properties
        ncmpi_inq_var(fin, varid, vname, &vtype, &ndims, dimids, &natts);
        ncmpi_inq_dimlen(fin, dimids[0], &varsize);
        ncmpi_inq_att(fin, varid, "_zip_originaldim", &vtype, &attlen);
        ndims = (int)attlen;
        if (maxndims < ndims){
            maxndims = ndims;
            dimids = (int*)realloc(dimids, maxndims * sizeof(int));
        }
        ncmpi_get_att_int(fin, varid, "_zip_originaldim", dimids);


        // Prepare data buffer
        if (varsize > datasize){
            datasize = varsize;
            data = (char*)realloc(data, datasize);
        }
        // Read data
        err = ncmpi_get_var_all(fin, varid, data, varsize, MPI_UNSIGNED_CHAR);

        // Perform decompression
        decompress(data, varsize, &ddata, &ddatasize);
        
        // Create decompressed variable
        ncmpi_redef(fout);
        ncmpi_def_var(fout, vname, vtype, ndims, dimids, &i);

        // Copy variable attributes
        for(i = 0; i < natts; i++){
            ncmpi_inq_attname (fin, varid, i, name);
            if (strncmp(name, "_zip", 4) != 0) {
                ncmpi_copy_att(fin, varid, name, fout, varid);
            }
        }
        
        // Write decomrpessed data
        ncmpi_get_att_int(fin, varid, "_zip_originaltype", &vtype);
        esize = NC_Type_size(vtype);
        btype = NC_Type_to_mpi_type(vtype);
        ncmpi_enddef(fout);
        ncmpi_put_var_all(fout, varid, ddata, ddatasize / esize, btype);

        // Free compressed data
        if (ddata != NULL){
            free(ddata);
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