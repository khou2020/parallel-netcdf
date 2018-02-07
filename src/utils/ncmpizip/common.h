#ifndef _NCMPIZIP_COMMON
#define _NCMPIZIP_COMMON

#define ZIP_PREFIX "zip_"

int compress(char *in, char *out);
int decompress(char *in, char *out);
#endif 