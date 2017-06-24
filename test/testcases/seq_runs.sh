#!/bin/sh

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

./put_all_kinds blocking
./iput_all_kinds nonblocking
export PNETCDF_HINTS="pnetcdf_log=enable"
./put_all_kinds blocking_log
./iput_all_kinds nonblocking_log
unset PNETCDF_HINTS
ncmpidiff blocking.cdf1 blocking_log.cdf1
ncmpidiff blocking.cdf2 blocking_log.cdf2
ncmpidiff blocking.cdf5 blocking_log.cdf5
ncmpidiff nonblocking.cdf1 nonblocking_log.cdf1
ncmpidiff nonblocking.cdf2 nonblocking_log.cdf2
ncmpidiff nonblocking.cdf5 nonblocking_log.cdf5
rm -f *.cdf1
rm -f *.cdf2
rm -f *.cdf5

