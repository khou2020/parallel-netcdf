#include "nc.h"
#include <linux/limits.h>

#define NC_LOG_TYPE_TEXT 1
#define NC_LOG_TYPE_SCHAR 2
#define NC_LOG_TYPE_UCHAR 3
#define NC_LOG_TYPE_SHORT 4
#define NC_LOG_TYPE_USHORT 5
#define NC_LOG_TYPE_INT 6
#define NC_LOG_TYPE_UINT 7
#define NC_LOG_TYPE_FLOAT 8
#define NC_LOG_TYPE_DOUBLE 9
#define NC_LOG_TYPE_LONGLONG 10
#define NC_LOG_TYPE_ULONGLONG 11
#define NC_LOG_TYPE_NATIVE 12

#define NC_LOG_API_KIND_VAR 1
#define NC_LOG_API_KIND_VAR1 2
#define NC_LOG_API_KIND_VARA 3
#define NC_LOG_API_KIND_VARS 4

#define NC_LOG_MAGIC_SIZE 8
#define NC_LOG_MAGIC "PnetCDF0"

#define NC_LOG_FORMAT_SIZE 8
#define NC_LOG_FORMAT_CDF_MAGIC "CDF0\0\0\0\0"
#define NC_LOG_FORMAT_HDF5_MAGIC "\211HDF\r\n\032\n"
#define NC_LOG_FORMAT_BP_MAGIC "BP\0\0\0\0\0\0"

#define NC_LOG_FALSE 0x00
#define NC_LOG_TRUE 0x01

/* PATH_MAX after padding to 4 byte allignment */
#if PATH_MAX % 4 == 0
#define NC_LOG_PATH_MAX PATH_MAX
#elif PATH_MAX % 4 == 1
#define NC_LOG_PATH_MAX PATH_MAX + 3
#elif PATH_MAX % 4 == 2
#define NC_LOG_PATH_MAX PATH_MAX + 2
#elif PATH_MAX % 4 == 3
#define NC_LOG_PATH_MAX PATH_MAX + 1
#endif

/* Metadata header
 * Variable named according to the spec
 * ToDo: Replace int with 4 byte integer variable if int is not 4 byte
 */
typedef struct NC_Log_metadataheader {
    char magic[NC_LOG_MAGIC_SIZE];
    char format[NC_LOG_FORMAT_SIZE];
    int big_endian;
    int is_external;
    MPI_Offset num_ranks;
    MPI_Offset rank_id;
    MPI_Offset entry_begin;
    MPI_Offset max_ndims;
    MPI_Offset num_entries;
    int basenamelen;
    char basename[1];   /* The hack to keep basename inside the structure */
} NC_Log_metadataheader;

/* Metadata entry header 
 * Variable named according to the spec
 * ToDo: Replace int with 4 byte integer variable if int is not 4 byte
 */
typedef struct NC_Log_metadataentry {
    MPI_Offset esize;
    int api_kind;
    int itype;
    int varid;
    int ndims;
    MPI_Offset data_off;
    MPI_Offset data_len;
} NC_Log_metadataentry;

/* Buffer structure */
typedef struct NC_Log_buffer {
    size_t nalloc;
    size_t nused;
    void* buffer;
} NC_Log_buffer;

/* Vector structure */
typedef struct NC_Log_sizearray {
    size_t nalloc;
    size_t nused;
    size_t* values;
} NC_Log_sizearray;

/* Log structure */
typedef struct NC_Log {
    char metalogpath[PATH_MAX];    /* path of metadata log */    
    char datalogpath[PATH_MAX];    /* path of data log */
    int rank;
    int np;
    int metalog_fd;    /* file handle of metadata log */
    int datalog_fd;    /* file handle of data log */
    int recdimid;
    size_t datalogsize;
    NC_Log_buffer metadata; /* In memory metadata buffer that mirrors the metadata log */
    NC_Log_sizearray entrydatasize;    /* Array of metadata entries */
    int isflushing;   /* If log is flushing */
    MPI_Offset total_data;
    MPI_Offset total_meta;
    MPI_Offset max_buffer;
    MPI_Offset numrecs;
    double flush_read_time;
    double flush_replay_time;
    double flush_total_time;
    double log_write_time;
    double log_write_data_time;
    double log_write_meta_time;
    double log_write_count_time;
    double log_total_time;
    double total_time;
} NC_Log;

int ncmpii_log_buffer_init(NC_Log_buffer * bp);
void ncmpii_log_buffer_free(NC_Log_buffer * bp);
char* ncmpii_log_buffer_alloc(NC_Log_buffer *bp, size_t size);
int ncmpii_log_sizearray_init(NC_Log_sizearray *sp);
void ncmpii_log_sizearray_free(NC_Log_sizearray *sp);
int ncmpii_log_sizearray_append(NC_Log_sizearray *sp, size_t size);
int log_flush(NC *ncp);
char *real_path(char* path, char* abs_path);

/*
int ncmpii_log_create(NC *ncp);
int ncmpii_log_put_var(NC *ncp, NC_var *varp, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Datatype buftype, int PackedSize);
int ncmpii_log_close(NC *ncp);
int ncmpii_log_flush(NC *ncp);
int ncmpii_log_enddef(NC *ncp);
*/
