#!/bin/sh

set -e

VALIDATOR=../../src/utils/ncmpivalid/ncmpivalid

for j in 0 1 ; do { \
    export PNETCDF_SAFE_MODE=$$j ; \
    for i in ${TESTPROGRAMS}; do { \
        ${TESTSEQRUN} ./$i            ${TESTOUTDIR}/testfile.nc ; \
        ${TESTSEQRUN} ${VALIDATOR} -q ${TESTOUTDIR}/testfile.nc ; \
} ; done ; } ; done

NCMPIGEN=../../src/utils/ncmpigen/ncmpigen
NCMPIDIFF=../../src/utils/ncmpidiff/ncmpidiff

echo "rm -f ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc"
rm -f ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc
echo "${TESTSEQRUN} ${NCMPIGEN} -v 2 -o ${TESTOUTDIR}/redef1.nc ${srcdir}/redef-good.ncdump"
${TESTSEQRUN} ${NCMPIGEN} -v 2 -o ${TESTOUTDIR}/redef1.nc ${srcdir}/redef-good.ncdump
echo "${TESTSEQRUN} ./redef1 ${TESTOUTDIR}/testfile.nc"
${TESTSEQRUN} ./redef1 ${TESTOUTDIR}/testfile.nc
echo "${TESTSEQRUN} ${NCMPIDIFF} -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc"
${TESTSEQRUN} ${NCMPIDIFF} -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc
echo "diff -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc"
diff -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc

./put_all_kinds ${TESTOUTDIR}/blocking
./iput_all_kinds ${TESTOUTDIR}/nonblocking

export PNETCDF_HINTS="nc_bb_driver=enable;nc_bb_overwrite=enable"

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

