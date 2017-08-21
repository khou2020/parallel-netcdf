define(`SCRIPT', dnl
`dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2 
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -o stage_$3.txt
#DW jobdw capacity=1024GB access_mode=striped type=scratch pool=sm_pool
#DW stage_out source=${DW_JOB_STRIPED}stage destination=$1/stage type=directory
rm -rf $1
mkdir $1
mkdir $1/stage
m4 -D OUTDIR=${DW_JOB_STRIPED}stage -D MAXLVL=LVL zgrd.m4 > zgrd_stage_$3.in
export MPICH_MPIIO_HINTS="*:romio_cb_write=disable"
mkdir ${DW_JOB_STRIPED}stage
echo ${DW_JOB_STRIPED}stage
srun -n $3 --export=MPICH_MPIIO_HINTS ./gcrm_io zgrd_stage_$3.in
')dnl
SCRIPT(EXPDIR,NNODE,NPROC,LVL)

