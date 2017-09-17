dnl Process this m4 file to produce 'C' language file.
dnl
dnl If you see this line, you can ignore the next one.
/* Do not edit this file. It is produced from the corresponding .m4 source */
dnl
/*********************************************************************
*
*  Copyright (C) 2017, Northwestern University and Argonne National Laboratory
*  See COPYRIGHT notice in top-level directory.
*
*********************************************************************/
/* $Id: ncmpilogdump.c 3078 2017-06-26 18:47Z khou $ */

include(`foreach.m4')dnl
include(`utils.m4')dnl
define(`upcase', `translit(`$*', `a-z', `A-Z')')

define(`PRINTAPIKIND',dnl
`dnl
                case NC_LOG_API_KIND_$2:
                    printf("$1");
                    break;
')dnl

define(`PRINTTYPE',dnl
`dnl
                case NC_LOG_TYPE_$2:
                    printf("_$1");
                    break;
')dnl

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <../../drivers/ncbbio/ncbbio_driver.h>
#include <pnetcdf.h>

int main(int argc, char *argv[]) {
    int i, j, k, fd, ret, err = NC_NOERR;
    size_t size, offset;
    FILE *fmeta, *fdata;
    struct stat metastat;
    struct stat datastat;
    MPI_Offset *start, *count, *stride;
    char *data, *head, *tail;
    char *Data, *Meta;
    NC_bb_metadataheader *Header;
    NC_bb_metadataentry *E;

    if (argc < 2){
        printf("Usage: ./ncmpilogdump <metadata log> [<data log>]\n");
        return 0;
    }

    /* Open metadata log and data log */
    fmeta = fdata = NULL;
    fmeta = fopen(argv[1], "rb+");
    if (fmeta == NULL) {
        err = NC_EFILE;
        goto ERROR;
    }
    
    /* Get file size */
    fd = fileno(fmeta);
    fstat(fd, &metastat);
   
    /* Allocate buffer */
    Meta = (char*)malloc(metastat.st_size);

    /* Read the metadata */
    ret = fread(Meta, metastat.st_size, 1, fmeta);
    if (ret != 1) {
        err = NC_EBADLOG;
        goto ERROR;
    }
 
    /* Open data log if path is given */
    if (argc > 2){
        fdata = fopen(argv[2], "rb+");
        if (fdata == NULL) {
            err = NC_EFILE;
            goto ERROR;
        }
        
        /* Get file size */
        fd = fileno(fdata);
        fstat(fd, &datastat);
        
        /* Allocate buffer */
        Data = (char*)malloc(datastat.st_size);
        /* Read the data */
        ret = fread(Data, datastat.st_size, 1, fdata);
        if (ret != 1) {
            err = NC_EBADLOG;
            goto ERROR;
        }
    }
    else{
        Data = NULL;
    }
   
    /* Parse header */
    Header = (NC_bb_metadataheader*)Meta;
    
    printf("Metadata log header:\n");
    printf("Magic:\t\t\t\t\t\t%.8s\n", Header->magic);
    printf("Format:\t\t\t\t\t\t%.8s\n", Header->format);
    printf("File name:\t\t\t\t\t%s\n", Header->basename);
    printf("Number of processes:\t\t%lld\n", Header->num_ranks);
    printf("Rank:\t\t\t\t\t\t%lld\n", Header->rank_id);
    if (Header->big_endian){
        printf("Endianness:\t\t\t\t\tbig endian\n");
    }
    else{
        printf("Endianness:\t\t\t\t\tlittle endian\n");
    }
    if (Header->is_external){
        printf("Data representation:\t\texternal\n");
    }
    else{
        printf("Data representation:\t\tinternal\n");
    }
    printf("Max number of dimensions:\t%lld\n", Header->max_ndims);
    printf("Begin of entry record:\t\t\%lld\n", Header->entry_begin);
    printf("Number of entries:\t\t\t%lld\n", Header->num_entries);
    
    printf("\nData log header:\n", Header->magic);
    printf("Magic:\t\t\t\t\t\t%.8s\n", Data);


    /* Parse entries */
    printf("\nLog entries:\n\n");
    offset = Header->entry_begin;
    for (j = 0; j < Header->num_entries; j++) {
        /* Parse metadata entry header */
        E = (NC_bb_metadataentry*)(Meta + offset);
        tail = (char*)E + sizeof(NC_bb_metadataentry);
        start = (MPI_Offset*)tail;
        count = start + E->ndims;
        stride = count + E->ndims;
       
        /* Original function call */
        printf("ncmpi_put_var");
        /* put_vara or put_vars */
        switch (E->api_kind){
foreach(`apikind', (`var1, var, vara, vars'), `PRINTAPIKIND(apikind, upcase(apikind))')dnl
            default:
                err = NC_EBADLOG;
                goto ERROR;
        }
        /* Data type of function call */
        switch (E->itype){
foreach(`vartype', (`text, uchar, schar, short, ushort, int, uint, float, double, longlong, ulonglong'), `PRINTTYPE(vartype, upcase(vartype))')dnl
            default:
                err = NC_EBADLOG;
                goto ERROR;
        }
        /* Start */
        printf("(ncid, %d, [ ", E->varid);
        for(i = 0; i < E->ndims; i++){
            printf("%lld", start[i]);
            if (i < (E->ndims - 1)){
                printf(", ");
            }
        }
        /* Count */
        printf(" ], [ ");
        for(i = 0; i < E->ndims; i++){
            printf("%lld", count[i]);
            if (i < (E->ndims - 1)){
                printf(", ");
            }
        }
        /* Stride */
        printf(" ], ");
        if (E->api_kind == NC_LOG_API_KIND_VARS){
            printf(" [ ");
            for(i = 0; i < E->ndims; i++){
                printf("%lld", stride[i]);
                if (i < (E->ndims - 1)){
                    printf(", ");
                }
            } 
            printf(" ], ");
        }
        printf("%08x);\n", E->data_off);
        
        /* Corresponding content in data log */
        if (Data != NULL){
            for (i = 0; i < E->data_len; i+= 16) {
                printf("%08x: ", E->data_off + i);
                for(k = 0; k < 16 && (i + k) < E->data_len; k++){
                    printf("%02x", (int)(Data[E->data_off + i + k]));
                    if(k & 1){
                        putchar(' ');
                    }
                }
                putchar('\n');
            }
            printf("\n");
        }

        /* Jump to next location */
        offset += E->esize;
    }

ERROR:
    if (fmeta != NULL){
        fclose(fmeta);
    }
    if (fdata != NULL){
        fclose(fdata);
    }
    if (Data != NULL){
        free(Data);
    }
    if (Meta != NULL){
        free(Meta);
    }
    switch(err){
        case NC_EFILE:
            printf("Can not open log file\n");
            break;
        case NC_EBADLOG:
            printf("Wrong log format\n");
            break;
        case NC_NOERR:
            break;
        default:
            printf("Unknown error\n");
            
    }
    return 0;
}
