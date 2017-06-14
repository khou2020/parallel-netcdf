#include "nc.h"
#include <stdint.h>
#include <pnetcdf.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define META_BUFFER_SIZE 1024 /* Size of initial metadata buffer */
#define META_OFFSET_BUFFER_SIZE 32 /* Size of initial metadata offset list */	
#define META_BUFFER_MULTIPLIER 20	/* When metadata buffer is full, we'll reallocate it to META_BUFFER_MULTIPLIER times the original size*/

/*
 * Create directories along the log file path
 * fopen does not create directory along the path is it does not exists, so we must create it before fopen
 * Code is copied from internet
 * IN	file_path:	path of the file
 * IN	mode: file permission mode of the newly created firectory	
 */
int mkpath(char* file_path, mode_t mode) {
	char* p;
	for (p = strchr(file_path + 1, '/'); p; p = strchr(p + 1, '/')) {
		*p = '\0';
		if (mkdir(file_path, mode) == -1) {
			if (errno != EEXIST) { *p = '/'; return -1; }
		}
		*p = '/';
	}
	return 0;
}

/*
 * Check if running on a little endian machine
 */
int IsLittleEndian() {
	uint32_t n = 1;
	char* b1 = (char*)&n;
	return (int)(*b1) == 1;
}

/*
 * Record metadata offset into the metadata offset list for fast retrival
 * This is similar to meta_alloc, but we allocate sizeof(size_t) for one offset and put it in instead of returns the buffer
 * IN	F:	log structure
 * IN	offset:	offset of the new metadata entry
 */
int AppendMetaOffset(NC_Log *F, size_t offset) {
	size_t* ret;

	/* Expand buffer if needed */
	if (F->MetaOffsetBufferSize < F->MetaOffsetHead + 1) {
		F->MetaOffsetBufferSize *= META_BUFFER_MULTIPLIER;
		ret = (size_t*)realloc(F->MetaOffset, F->MetaOffsetBufferSize * sizeof(size_t));
		if (ret == NULL) {
			return NC_LOG_ERR_MEM_ALLOC;
		}
		F->MetaOffset = ret;
	}

	F->MetaOffset[F->MetaOffsetHead++] = offset;
	
	return NC_LOG_SUCCESS;
}

/*
 * Allocate space in the metadata buffer
 * This function works as malloc in the metadata buffer
 * IN	F:	log structure
 * IN	size:	size required in the buffer
 */
char* meta_alloc(NC_Log *F, size_t size) {
	char* ret;

	/* Expand buffer if needed */
	if (F->MetaBufferSize < F->MetaHead + size) {
		while (F->MetaBufferSize < F->MetaHead + size) {
			F->MetaBufferSize *= META_BUFFER_MULTIPLIER;
		}
		/* ret is used to temporaryly hold the allocated buffer so we don't lose F->Metadata if allocation fails */
		ret = (char*)realloc(F->Metadata, F->MetaBufferSize);
		if (ret == NULL) {
			return ret;
		}
		F->Metadata = ret;
	}
	ret = F->Metadata + F->MetaHead;
	F->MetaHead += size;
	return ret;
}

/*
 * Initialize the log structure
 * This function initialize the information required to operate the log, but it does not create log file
 * IN	comm:	communicator passed to ncmpi_open
 * IN	path:	path of the CDF file
 * IN	bufferdir:	root directory to store log file
 * IN	F:	log structure to be initialized
 */
int init_file_metadata(MPI_Comm comm, const char* path, const char* bufferdir, NC_Log *F) {
	int rank, np, ret;
	char logpath[NC_LOG_PATH_MAX];
	NC_Log_metadataheader H;

	ret = MPI_Comm_rank(comm, &rank);
	if (ret != MPI_SUCCESS) {
		return ret;
	}
	ret = MPI_Comm_size(MPI_COMM_WORLD, &np);
	if (ret != MPI_SUCCESS) {
		return ret;
	}

	/* Fill up the metadata log header */
	memset(H.magic, 0, sizeof(H.magic));
	memset(H.format, 0, sizeof(H.format));
	strncpy(H.magic, NC_LOG_MAGIC, sizeof(H.magic));
	strncpy(H.format, NC_LOG_FORMAT_CDF_MAGIC, sizeof(H.format));
	H.rank_id = rank;
	H.num_ranks = np;
	H.is_external = 1;	/* Without convertion beefore logging */
	/* Determine endianess */
	if (IsLittleEndian()) {
		H.big_endian = NC_LOG_FALSE;
	}
	else {
		H.big_endian = NC_LOG_TRUE;
	}
	H.num_entries = 0;	/* To be modified later */
	H.max_ndims = 0;	/* To be modified later */
	strncpy(H.basename, path, sizeof(H.basename));
	H.entry_begin = sizeof(H);

	/* Fill up the log structure */
	/* Determine log file name */
	if (H.basename[0] == '/' || H.basename[0] == '\\') {	/* If we have absolute path */
		sprintf(F->MetaPath, "%s%s_%d.meta.bin", bufferdir, H.basename, rank);
		sprintf(F->DataPath, "%s%s_%d.data.bin", bufferdir, H.basename, rank);
	}
	else{	/* If we have relative path */
		sprintf(F->MetaPath, "%s/%s_%d.meta.bin", bufferdir, H.basename, rank);
		sprintf(F->DataPath, "%s/%s_%d.data.bin", bufferdir, H.basename, rank);
	}
	strncpy(F->Path, path, sizeof(F->Path));
	F->MaxSize = 0;
	F->MetaBufferSize = META_BUFFER_SIZE;
	F->Metadata = (char*)malloc(F->MetaBufferSize); /* Allocate metadata buffer */
	F->MetaHead = 0;
	F->MetaOffsetBufferSize = META_OFFSET_BUFFER_SIZE;
	F->MetaOffset = (size_t*)malloc(F->MetaOffsetBufferSize * sizeof(size_t)); /* Allocate list of metadata pointer */
	F->MetaOffsetHead = 0;
	F->Communitator = comm;
	F->MetaHeader = H;
	F->Flushing = 0;

	return NC_LOG_SUCCESS;
}

/*
 * This function creates the log file
 * IN	F:	log structure
 */
int create_log_file(NC_Log *F) {
	char* buffer;
	size_t size;
	int32_t namelen;

	/* Write metadata header to metadata buffer */
	buffer = meta_alloc(F, F->MetaHeader.entry_begin);
	size = 0;
	memcpy(buffer + size, &F->MetaHeader, sizeof(F->MetaHeader));
	size += sizeof(F->MetaHeader);

	if (size != F->MetaHeader.entry_begin) {
		return NC_LOG_ERR_UNKNOWN;
	}

	/* Create log files */
	F->DataLog = F->MetaLog = NULL;
	/* TODO: use separate directory for absolute and relative path to prevent conflict */
	mkpath(F->MetaPath, 0744); /* Log file path may contain directory */
	F->MetaLog = fopen(F->MetaPath, "wb+");
	F->DataLog = fopen(F->DataPath, "wb+");
	if (F->DataLog == NULL || F->MetaLog == NULL) {
		return NC_LOG_ERR_LOG_CREATE;
	}
	/* Disable buffering */
	setvbuf(F->DataLog, NULL, _IONBF, 0);
	setvbuf(F->MetaLog, NULL, _IONBF, 0);

	/* Write metadata header to file */
	fseek(F->MetaLog, 0, SEEK_SET);
	fwrite(buffer, size, 1, F->MetaLog);

	/* Write data header to file */
	fseek(F->DataLog, 0, SEEK_SET);
	fwrite("PnetCDF0", 8, 1, F->DataLog);

	return NC_LOG_SUCCESS;
}

/*
 * Create a new log structure
 * IN	comm:	communicator passed to ncmpi_open
 * IN	path:	path of the CDF file
 * IN	BufferDir:	root directory to store log file
 * IN	Parent:	NC structure that will host the log structure
 * OUT	nclogp:	Initialized log structure 
 */
int ncmpii_log_create(MPI_Comm comm, const char* path, const char* BufferDir, NC *Parent, NC_Log **nclogp) {
	int ret;
	NC_Log *F;

	F = (NC_Log*)malloc(sizeof(NC_Log));
	
	F->Parent = Parent; /* The NC structure holding the log */

	/* Initialize log structure */
	ret = init_file_metadata(comm, path, BufferDir, F);
	if (ret != NC_LOG_SUCCESS) {
		goto ERROR;
	}

	/* Create log files */
	ret = create_log_file(F);
	if (ret != NC_LOG_SUCCESS) {
		goto ERROR;
	}

	*nclogp = F;

ERROR:;
	if (ret != NC_LOG_SUCCESS) {
		free(F);
	}
	return ret;
}

/*
 * Open an existing log file associate with the log structure
 * This may happens when recover from a crash or when replay is delayed (currently not supported)
 * In addition to opening the file, we also need to restore previous status based on the log file
 * IN	F:	log structure
 */
int open_log_file(NC_Log *F) {
	int i, fd, namelen;
	char* buffer;
	size_t ret, nrec, size, offset;
	struct stat metastat;
	NC_Log_metadataentry *E;

	/* Open existing log file */
	F->DataLog = F->MetaLog = NULL;
	F->MetaLog = fopen(F->MetaPath, "rb+");
	F->DataLog = fopen(F->DataPath, "rb+");
	if (F->DataLog == NULL || F->MetaLog == NULL) {
		return NC_LOG_ERR_LOG_CREATE;
	}

	/* Get file size */
	fd = fileno(F->MetaLog);
	fstat(fd, &metastat);

	/* The metadata is mirrored in memory, we need to populate the memory buffer with exiting metadata in the file */
	/* Preparing the buffer
	 */
	if (F->MetaBufferSize < metastat.st_size) {
		if (F->MetaBufferSize <= 0) {
			F->MetaBufferSize = META_BUFFER_SIZE;
		}
		while (F->MetaBufferSize < metastat.st_size) {	/* Calculating the required size */
			F->MetaBufferSize *= META_BUFFER_MULTIPLIER;
		}
		/* Allocate memory buffer */
		F->Metadata = (char*)realloc(F->Metadata, F->MetaBufferSize);
	}

	/* Read metadata to memory buffer */
	ret = fread(F->Metadata, metastat.st_size, 1, F->MetaLog);
	if (ret == 0) {
		return NC_LOG_ERR_LOG_CORRUPTED;
	}
	/* Restore the metadata header in the log structure */
	size = 0;
	memcpy(buffer + size, &F->MetaHeader, sizeof(F->MetaHeader));
	size += sizeof(F->MetaHeader);

	/* Restore the metadata record list */
	offset = F->MetaHeader.entry_begin;
	for(i = 0; i < F->MetaHeader.num_entries; i++) {
		/* Record the offset */
		AppendMetaOffset(F, offset);

		/* Get metaheader */
		E = (NC_Log_metadataentry*)(F->Metadata + offset);

		/* Update datasize info */
		if (F->MaxSize < E->data_len) {
			F->MaxSize = E->data_len;
		}

		offset += E->esize;	/* Jump to next location */
	}
	F->MetaHead = offset;

	fflush(F->MetaLog);	/* Switch to write mode */	

	return NC_LOG_SUCCESS;
}

/*
 * Create new log structure of existing CDF file
 * The old log file, if exists, is used 
 * Used by ncmpi_open()
 * IN	comm:	communicator passed to ncmpi_open
 * IN	path:	path of the CDF file
 * IN	BufferDir:	root directory to store log file
 * IN	Parent:	NC structure that will host the log structure
 * OUT	nclogp:	Initialized log structure
 */
int ncmpii_log_open(MPI_Comm comm, const char* path, const char* BufferDir, NC* Parent, NC_Log **nclogp) {
	int ret;
	NC_Log *F;

	F = (NC_Log*)malloc(sizeof(NC_Log));

	F->Parent = Parent;

	/* Initialize log info */
	ret = init_file_metadata(comm, path, BufferDir, F);
	if (ret != NC_LOG_SUCCESS) {
		return ret;
	}

	/* Prepare log file */
	F->DataLog = F->MetaLog = NULL;
	if (access(F->MetaPath, F_OK) != -1) { /* If log file already exists */
		if (access(F->DataPath, F_OK) != -1) { /* Data log should also exists */
			/* Continue on previous status when log already exists (abnormal terminstion previously) */
			ret = open_log_file(F);
			if (ret != NC_LOG_SUCCESS) {
				return ret;
			}
		}
		else {
			return NC_LOG_ERR_LOG_CORRUPTED;	/* 2 Logs must exist simultenously */
		}
	}
	else {
		/* Create new log files */
		ret = create_log_file(F);
		if (ret != NC_LOG_SUCCESS) {
			return ret;
		}
	}

	*nclogp = F;

ERROR:;
	if (ret != NC_LOG_SUCCESS) {
		free(F);
	}
	return ret;
}

/*
 * Commit log file into CDF file
 * Meta data is stored in memory, metalog is only used for restoration after abnormal shutdown
 * IN	nclogp:	log structure
 */
int flush_log(NC_Log *nclogp) {
	int i, ret, fd;
	int *req, *stat;
	size_t nrec, size, offset;
	struct stat metastat;
	uint32_t hash;
	NC_Log *F = nclogp;
	NC_Log_metadataentry *E;
	NC_var *varp;
	MPI_Offset *start, *count, *stride;
	MPI_Datatype buftype;
	char *data, *head, *tail;
	struct stat datastat;

	nclogp->Flushing = 1;

	/* Read datalog in memory */
	/* Get file size */
	fd = fileno(F->MetaLog);
	fstat(fd, &datastat);
	/* Prepare data buffer */
	data = (char*)malloc(datastat.st_size);
	fseek(F->DataLog, 0, SEEK_SET);	/* Seek to the start of data log */
	fread(data, datastat.st_size, 1, F->DataLog);	/* Read data to buffer */

	fflush(F->DataLog);	/* Change to read mode */

	/* Handle to non-blocking requrest */
	req = (int*)malloc(sizeof(int) * F->MetaHeader.num_entries);
	stat = (int*)malloc(sizeof(int) * F->MetaHeader.num_entries);
	
	/* Iterate through meta log entries */
	head = F->Metadata + F->MetaHeader.entry_begin;
	tail = head + sizeof(NC_Log_metadataentry);
	for (i = 0; i < F->MetaHeader.num_entries; i++) {
		E = (NC_Log_metadataentry*)head;	/* Metadata header */

		/* start, count, stride */
		start = (MPI_Offset*)tail;
		count = start + E->ndims;
		stride = count + E->ndims;

		/* Convert from log type to MPI type used by pnetcdf library
		 * Log spec has different enum of types than MPI
		 */
		switch (E->itype) {
		case NC_LOG_TYPE_NATIVE:
			buftype = MPI_CHAR;
			break;
		case NC_LOG_TYPE_TEXT:
			buftype = MPI_CHAR;
			break;
		case NC_LOG_TYPE_SCHAR:
			buftype = MPI_SIGNED_CHAR;
			break;
		case NC_LOG_TYPE_SHORT:
			buftype = MPI_SHORT;
			break;
		case NC_LOG_TYPE_INT:
			buftype = MPI_INT;
			break;
		case NC_LOG_TYPE_FLOAT:
			buftype = MPI_FLOAT;
			break;
		case NC_LOG_TYPE_DOUBLE:
			buftype = MPI_DOUBLE;
			break;
		case NC_LOG_TYPE_UCHAR:
			buftype = MPI_UNSIGNED_CHAR;
			break;
		case NC_LOG_TYPE_USHORT:
			buftype = MPI_UNSIGNED_SHORT;
			break;
		case NC_LOG_TYPE_UINT:
			buftype = MPI_UNSIGNED;
			break;
		case NC_LOG_TYPE_LONGLONG:
			buftype = MPI_LONG_LONG_INT;
			break;
		case NC_LOG_TYPE_ULONGLONG:
			buftype = MPI_UNSIGNED_LONG_LONG;
			break;
		default:
			buftype = MPI_CHAR;
		}
		
		/* Determine API_Kind */
		if (E->api_kind == NC_LOG_API_KIND_VARA){
			stride = NULL;	
		}
		
		/* Play event */
		ret = ncmpii_NC_lookupvar(nclogp->Parent, E->varid, &varp);
		if (ret != NC_NOERR){
			return ret;
		}
		req[i] = NC_REQ_NULL;
		ret = ncmpii_igetput_varm(nclogp->Parent, varp, start, count, stride, NULL, (void*)(data + E->data_off), -1, buftype, req + i, WRITE_REQ, 0, 0);
		if (ret != NC_NOERR) {
			return ret;
		}

		/* Move to next record */
		head += E->esize;
		tail = head + sizeof(NC_Log_metadataentry);
	}

	/* Collective wait */
	ret = ncmpii_wait(nclogp->Parent, F->MetaHeader.num_entries, req, stat, COLL_IO);
	if (ret != NC_NOERR) {
		return ret;
	}
	/* Return the first error encountered */
	for (i = 0; i < F->MetaHeader.num_entries; i++) {
		if (stat[i] != NC_NOERR) {
			return stat[i];
		}
	}

	fflush(F->DataLog);	/* Change to write mode */
	fflush(F->MetaLog);	/* Change to write mode */

	free(data);
	free(req);
	free(stat);
	
	nclogp->Flushing = 0;

	return NC_LOG_SUCCESS;
}

/*
 * Flush the log to CDF file and clean up the log structure 
 * Used by ncmpi_close()
 * IN	nclogp:	log structure
 */
int ncmpii_log_close(NC_Log *nclogp) {
	NC_Log *F = nclogp;

	/* Commit to CDF file */
	flush_log(nclogp);

	/* Close log file */
	fclose(F->MetaLog);
	fclose(F->DataLog);

	/* Delete log files */
	if (nclogp->DeleteOnClose){
		remove(F->DataPath);
		remove(F->MetaPath);
	}

	/* Free meta data buffer */
	free(F->Metadata);
	free(F->MetaOffset);

	/* Delete log structure */
	free(F);

	return NC_LOG_SUCCESS;
}

/*
 * This function is not used
 * For fixed size log
 */
int ncmpii_log_update_max_ndim(NC_Log *nclogp, int64_t ndims) {
	return NC_LOG_SUCCESS;
}

/*
 * Write metadata to memory buffer as well as metadata log
 * IN	F: log structure
 * IN	E: metadata entry header
 * IN	start: start in put_var* call
 * IN	count: count in put_var* call
 * IN	stride: stride in put_var* call
 */
int WriteMeta(NC_Log *F, NC_Log_metadataentry E, const int64_t start[], const int64_t count[], const int64_t stride[]) {
	size_t size, vsize;
	char *buffer;
	
	/* Seek needs old Metahead */
	fseek(F->MetaLog, F->MetaHead, SEEK_SET);	/* Note: partial record may exist, so can not use EOF as start point */

	/* Allocate space in metadata buffer */
	buffer = meta_alloc(F, E.esize);
	if (buffer == NULL) {
		return NC_LOG_ERR_MEM_ALLOC;
	}

	size = 0;
	/* Metadata entry header */
	memcpy(buffer + size, &E, sizeof(E));
	size += sizeof(E);
	/* Variable length additional data */
	/* Length of start, count, stride vector */
	vsize = sizeof(int64_t) * E.ndims;
	/* Start */
	if (start != NULL) {
		memcpy(buffer + size, start, vsize);
	}
	size += vsize;
	/* Count */
	if (count != NULL) {
		memcpy(buffer + size, count, vsize);
	}
	size += vsize;
	/* Stride */
	if (stride != NULL) {
		memcpy(buffer + size, stride, vsize);
	}
	size += vsize;
	
	/* Total size should match that recorded in the entry heaer */
	if (size != E.esize) {
		return NC_LOG_ERR_UNKNOWN;
	}

	/* Record offset */
	AppendMetaOffset(F, F->MetaHead);

	/* Write to disk and update the log structure */
	fwrite(buffer, size, 1, F->MetaLog);
	F->MetaHeader.num_entries++;
	fseek(F->MetaLog, sizeof(F->MetaHeader) - sizeof(F->MetaHeader.basename) - sizeof(F->MetaHeader.num_entries), SEEK_SET);	/* Note: location need to be updated when struct change */
	fwrite(&F->MetaHeader.num_entries, sizeof(F->MetaHeader.num_entries), 1, F->MetaLog);	/* This marks the completion of the record */

	return NC_LOG_SUCCESS;
}

/*
 * Pack imaped buffer into continuous buffer
 * This function is unused 
 */
char* BufferUnroll(int dim, const MPI_Offset count[], const MPI_Offset imap[], const void *ip, size_t unit) {
	int i, ret;
	char *buffer;
	MPI_Offset *idx;
	size_t size, src, dst;

	idx = (MPI_Offset*)malloc((dim + 1) * sizeof(MPI_Offset));

	size = 0;
	for (i = 0; i < dim; i++) {
		size *= count[i];
	}
	buffer = (char*)malloc(size * unit);

	/* Unroll Buffer */
	src = 0;
	memset(buffer, 0, size * unit);
	while (idx[0] < count[0]) {
		dst = 0;
		for (i = 0; i < dim; i++) {
			dst += idx[i] * imap[i];
		}

		memcpy(buffer + src, ip + (dst * unit), unit);	/* Copy one entry */

		/* Jump to next index */
		idx[dim - 1]++;
		for (i = dim - 1; i > 0; i--) {	/* Note: copied in order of buffers address, idx carry order can not be reversed */
			if (idx[i] >= count[i]) {
				idx[i] = 0;
				idx[i - 1]++;
			}
		}
		src += unit;
	}

	free(idx);

	return buffer;	/* Note: User must free this buffer */
}

/*
 * Retrieve variable id given NC_var and NC
 * IN	ncp:	NC structure
 * IN	varp:	NC_var structure
 */
int get_varid(NC *ncp, NC_var *varp){
	int i;
	NC_vararray ncap = ncp->vars;
	
	/* search through the variable list for the same address */
	for(i = 0; i < ncap.ndefined; i++){
		if (ncap.value[i] == varp){
			return i;
		}
	}

	return -1;
}

/*
 * Prepare a single log entry to be write to log
 * Used by ncmpii_getput_varm
 * IN	nclogp:	log structure to log this entry
 * IN	varp:	NC_var structure associate to this entry
 * IN	start: start in put_var* call
 * IN	count: count in put_var* call
 * IN	stride: stride in put_var* call
 * IN	bur:	buffer of data to write
 * IN	buftype:	buftype as in ncmpii_getput_varm, MPI_PACKED indicate a flexible api
 * IN	PackedSize:	Size of buf in byte, only valid when buftype is MPI_PACKED
 */
int ncmpii_log_put_var(NC_Log *nclogp, NC_var *varp, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Datatype buftype, int PackedSize){
	int i, size, ret, vid, dim;
	int itype;	/* Type used in log file */
	MPI_Offset *Count;

	/* Get variable id and dimension */
	dim = varp->ndims;
	vid = get_varid(nclogp->Parent, varp);

	/* We may need to modify count, make a copy */
	Count = (MPI_Offset*)malloc(sizeof(MPI_Offset) * dim);
	for (i = 0; i < varp->ndims; i++) {
		Count[i] = count[i];
	}
		
	/* Convert to log types */
	switch (buftype) {
	case MPI_PACKED:	/* Flexible api */
		/* Treat packed data as text (char) 
		 * The size indicate by count is in unit of original type
		 * We multiply the count of fastest changing dimension to match that of the buffer size
		 */
		itype = NC_LOG_TYPE_TEXT;
		size = 1;
		for (i = 0; i < varp->ndims; i++) {
			size *= Count[i];
		}
		/* Size indicate by original count */
		if (size > 0) {
			/* Modify count to reflect size of original type */ 
			Count[varp->ndims - 1] *= PackedSize / size;
		}
		break;
	case MPI_CHAR:	/* put_*_char */
		itype = NC_LOG_TYPE_SCHAR;
		break;
	case MPI_UNSIGNED_CHAR:	/* put_*_uchar */
		itype = NC_LOG_TYPE_UCHAR;
		break;
	case MPI_BYTE: /* Not corresponding to any api, not used */
		itype = NC_LOG_TYPE_TEXT;
		break;
	case MPI_SHORT:	/* put_*_short */
		itype = NC_LOG_TYPE_SHORT;
		break;
	case MPI_UNSIGNED_SHORT: /* put_*_ushort */
		itype = NC_LOG_TYPE_USHORT;
		break;
	case MPI_INT: /* put_*_int */
		itype = NC_LOG_TYPE_INT;
		break;
	case MPI_UNSIGNED: /* put_*_uint */
		itype = NC_LOG_TYPE_UINT;
		break;
	case MPI_FLOAT: /* put_*_float */
		itype = NC_LOG_TYPE_FLOAT;
		break;
	case MPI_DOUBLE: /* put_*_double */
		itype = NC_LOG_TYPE_DOUBLE;
		break;
	case MPI_LONG_LONG_INT: /* put_*_longlong */
		itype = NC_LOG_TYPE_LONGLONG;
		break;
	case MPI_UNSIGNED_LONG_LONG: /* put_*_ulonglong */
		itype = NC_LOG_TYPE_ULONGLONG;
		break;
	default:
		itype = NC_LOG_TYPE_NATIVE; /* Unrecognized type */
		break;
	}

	if (stride == NULL){
		ret = ncmpii_logi_put_var(nclogp, NC_LOG_API_KIND_VARA, itype, vid, dim, start, Count, stride, buf); 
	}
	else{
		ret = ncmpii_logi_put_var(nclogp, NC_LOG_API_KIND_VARS, itype, vid, dim, start, Count, stride, buf); 
	}

	return ret;
}

/*
 * write a single log entry to log
 * Used by ncmpii_getput_varm
 * IN	nclogp:	log structure to log this entry
 * IN	apikind:	NC_var structure associate to this entry
 * IN	varid:	id of the variable
 * IN	ndim:	dimension of the variable
 * IN	start: start in put_var* call
 * IN	count: count in put_var* call
 * IN	stride: stride in put_var* call
 * IN	ip:	buffer of data to write
 */
int ncmpii_logi_put_var(NC_Log *nclogp, int32_t api_kind, int32_t itype, int varid, int ndim, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const void *ip) {
	int i, j, tsize;
	int dimids[NC_MAX_DIMS];
	MPI_Offset usize, dsize, asize, metasize;
	NC_Log_metadataentry E;
	NC_Log *F = nclogp;
	int64_t *Start, *Count, *Stride;

	/* By the log spec, start, count, stride is 64 bit int
	 * If MPI offset is not 64 bit, convert it
	 */
	if (sizeof(MPI_Offset) == sizeof(int64_t)) {
		Start = (int64_t*)start;
		Count = (int64_t*)count;
		Stride = (int64_t*)stride;
	}
	else {
		Start = (int64_t*)malloc(ndim * sizeof(int64_t));
		Count = (int64_t*)malloc(ndim * sizeof(int64_t));
		Stride = (int64_t*)malloc(ndim * sizeof(int64_t));
		for(i = 0; i < ndim; i++){
			Start[i] = (int64_t)start[i];
			Count[i] = (int64_t)count[i];
			Stride[i] = (int64_t)stride[i];
		}
	}

	/* Prepare metadata entry header */
	E.data_off = ftell(F->DataLog);	/* Record address in data log */
	E.api_kind = api_kind;
	E.itype = itype;
	E.varid = varid;
	E.ndims = ndim;
	E.esize = sizeof(NC_Log_metadataentry) + ndim * 3 * sizeof(int64_t);	/* Size of metadata including variable size additional data

	/* Calculate data size */
	/* Unit of the datatype in bytes */
	switch (itype) {
	case NC_LOG_TYPE_TEXT:
		usize = sizeof(char);
		break;
	case NC_LOG_TYPE_SCHAR:
		usize = sizeof(signed char);
		break;
	case NC_LOG_TYPE_UCHAR:
		usize = sizeof(unsigned char);
		break;
	case NC_LOG_TYPE_SHORT:
		usize = sizeof(short);
		break;
	case NC_LOG_TYPE_USHORT:
		usize = sizeof(unsigned short);
		break;
	case NC_LOG_TYPE_INT:
		usize = sizeof(int);
		break;
	case NC_LOG_TYPE_UINT:
		usize = sizeof(unsigned int);
		break;
	case NC_LOG_TYPE_FLOAT:
		usize = sizeof(float);
		break;
	case NC_LOG_TYPE_DOUBLE:
		usize = sizeof(double);
		break;
	case NC_LOG_TYPE_LONGLONG:
		usize = sizeof(long long);
		break;
	case NC_LOG_TYPE_ULONGLONG:
		usize = sizeof(unsigned long long);
		break;
	default:	/* Should never executed */
		usize = 0;
		break;
	}
	/* Size indicate by count */
	asize = 1;
	for (i = 0; i < E.ndims; i++) {
		asize *= Count[i];
	}
	E.data_len = asize * usize; /* Buffer size in byte */

	/* Record the maximun buffer size required in the commiting stage */
	if (E.data_len > F->MaxSize) {
		F->MaxSize = E.data_len;
	}

	/* Note: Metadata record indicate completion, so data must go first */

	/* Write data log */
	fwrite(ip, E.data_len, 1, F->DataLog);

	/* Write meta log */
	WriteMeta(F, E, Start, Count, Stride);

	if (sizeof(MPI_Offset) != sizeof(int64_t)) {
		free(Start);
		free(Count);
		free(Stride);
	}

	return NC_LOG_SUCCESS;
}

/*
 * Commit the log into cdf file and delete the log
 * User can call this to force a commit without closing
 * It work by flush and re-initialize the log structure
 * This function is not used
 * IN	nclogp:	log structure
 */
int ncmpii_log_flush(NC_Log *nclogp) {
	int ret;
	NC_Log *F = nclogp;

	if ((ret = flush_log(nclogp)) != NC_LOG_SUCCESS) {
		return ret;
	}

	/* Reset metadata buffer */
	F->MetaHead = 0;
	F->MetaOffsetHead = 0;
	F->MetaHeader.num_entries = 0;
	
	/* Create new log file */
	ret = create_log_file(F);
	if (ret != NC_LOG_SUCCESS) {
		return ret;
	}

	return NC_LOG_SUCCESS;
}

