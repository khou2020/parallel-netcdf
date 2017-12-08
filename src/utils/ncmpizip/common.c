#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pnetcdf.h>
#include <string.h>

size_t NC_Type_size(nc_type type){
    if (type == NC_CHAR || type == NC_BYTE || type == NC_UBYTE){
        return 1;
    }
    else if (type == NC_SHORT || type == NC_USHORT){
        return 2;
    }
    else if (type == NC_INT || type == NC_UINT || type == NC_FLOAT){
        return 4;
    }
    else if (type == NC_DOUBLE || type == NC_INT64 || type == NC_UINT64){
        return 8;
    }
    return 0;
}

MPI_Datatype NC_Type_to_mpi_type(nc_type type){
    if (type == NC_CHAR || type == NC_BYTE || type == NC_UBYTE){
        return MPI_BYTE;
    }
    else if (type == NC_SHORT || type == NC_USHORT){
        return MPI_SHORT;
    }
    else if (type == NC_INT || type == NC_UINT || type == NC_FLOAT){
        return MPI_INT;
    }
    else if (type == NC_DOUBLE || type == NC_INT64 || type == NC_UINT64){
        return MPI_DATATYPE_NULL;
    }
    return MPI_DATATYPE_NULL;
}

int compress(void *in, size_t len, void **out, size_t *clen){
    *out = malloc(len);
    memcpy(*out, in, len);
    *clen = len;
    return 0;
}

int decompress(void *in, size_t len, void **out, size_t *dlen){
    *out = malloc(len);
    memcpy(*out, in, len);
    *dlen = len;
    return 0;
}