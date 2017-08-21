#!/bin/bash
#WD=$3
WD="/global/cscratch1/sd/khl7265/FS_128_4M/flash"
m4 -D EXPDIR=${WD} -D NNODE=$1 -D NPROC=$2 log.m4 > log_$2.sl
#m4 -D OUTDIR=${WD}/log -D MAXLVL=$3 zgrd.m4 > zgrd_log_$2.in
m4 -D EXPDIR=${WD} -D NNODE=$1 -D NPROC=$2 nolog.m4 > nolog_$2.sl
#m4 -D OUTDIR=${WD}/nolog -D MAXLVL=$3 zgrd.m4 > zgrd_nolog_$2.in
#m4 -D EXPDIR=${WD} -D NNODE=$1 -D NPROC=$2 -D LVL=$3 stage.m4 > stage_$2.sl

