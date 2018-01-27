#!/bin/sh
#
# Copyright (C) 2003, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#

# Exit immediately if a command exits with a non-zero status.
set -e

VALIDATOR=../../src/utils/ncvalidator/ncvalidator

${TESTSEQRUN} ./put_all_kinds ${TESTOUTDIR}/put_all_kinds.nc
${TESTSEQRUN} ${VALIDATOR} -q ${TESTOUTDIR}/put_all_kinds.nc.cdf1
${TESTSEQRUN} ${VALIDATOR} -q ${TESTOUTDIR}/put_all_kinds.nc.cdf2
${TESTSEQRUN} ${VALIDATOR} -q ${TESTOUTDIR}/put_all_kinds.nc.cdf5

NCMPIGEN=../../src/utils/ncmpigen/ncmpigen
NCMPIDIFF=../../src/utils/ncmpidiff/ncmpidiff

# remove the file system type prefix name if there is any.
OUT_PATH=`echo "$TESTOUTDIR" | cut -d: -f2-`

rm -f ${OUT_PATH}/testfile.nc ${OUT_PATH}/redef1.nc
${TESTSEQRUN} ${NCMPIGEN} -v 2 -o ${TESTOUTDIR}/redef1.nc ${srcdir}/redef-good.ncdump
echo "${TESTSEQRUN} ./redef1 ${TESTOUTDIR}/testfile.nc"
${TESTSEQRUN} ./redef1 ${TESTOUTDIR}/testfile.nc
echo "${TESTSEQRUN} ${NCMPIDIFF} -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc"
${TESTSEQRUN} ${NCMPIDIFF} -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc
diff -q ${OUT_PATH}/testfile.nc ${OUT_PATH}/redef1.nc

${TESTSEQRUN} ${VALIDATOR} -q ${TESTOUTDIR}/testfile.nc

./put_all_kinds ${TESTOUTDIR}/blocking
./iput_all_kinds ${TESTOUTDIR}/nonblocking

export PNETCDF_HINTS="nc_dw_driver=enable;nc_dw_overwrite=enable"

for i in $TESTPROGRAMS; do { 
    $TESTSEQRUN ./$i $TESTOUTDIR/testfile.nc ;
} ; done

./put_all_kinds ${TESTOUTDIR}/blocking_log
./iput_all_kinds ${TESTOUTDIR}/nonblocking_log

unset PNETCDF_HINTS

for i in blocking nonblocking ; do { \
    for j in cdf1 cdf2 cdf5; do { \
        ${TESTSEQRUN} ${NCMPIDIFF} -q ${TESTOUTDIR}/${i}.${j} ${TESTOUTDIR}/${i}_log.${j} ; \
    } ; done \
} ; done

for i in blocking nonblocking blocking_log nonblocking_log ; do { \
    for j in cdf1 cdf2 cdf5 ; do { \
        rm -f ${TESTOUTDIR}/${i}.${j} ; \
    } ; done \
} ; done

${TESTSEQRUN} ${VALIDATOR} -q ${TESTOUTDIR}/testfile.nc

