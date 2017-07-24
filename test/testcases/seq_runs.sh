#!/bin/sh

rm -f *.data
rm -f *.meta

set -e

for i in $TESTPROGRAMS; do { \
        $TESTSEQRUN ./$i $TESTOUTDIR/testfile.nc ; \
} ; done

NCMPIGEN=../../src/utils/ncmpigen/ncmpigen
NCMPIDIFF=../../src/utils/ncmpidiff/ncmpidiff

rm -f ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc
${TESTSEQRUN} ${NCMPIGEN} -v 2 -o ${TESTOUTDIR}/redef1.nc ${srcdir}/redef-good.ncdump
${TESTSEQRUN} ./redef1 ${TESTOUTDIR}/testfile.nc
${TESTSEQRUN} ${NCMPIDIFF} -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc
diff -q ${TESTOUTDIR}/testfile.nc ${TESTOUTDIR}/redef1.nc

./put_all_kinds ${TESTOUTDIR}/blocking
./iput_all_kinds ${TESTOUTDIR}/nonblocking

export PNETCDF_HINTS="pnetcdf_log=1"

for i in $TESTPROGRAMS; do { 
        echo $i
        # The only 2 testcase not supported by log is test_erange and erange_fill
        if [ $i ~= "test_erange" ] || [ $i ~= "erange_fill" ]; then
            $TESTSEQRUN ./$i $TESTOUTDIR/testfile.nc ;
        fi
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

