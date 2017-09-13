changequote(`[', `]')dnl
define([SCRIPT], dnl
[dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2 
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -o stage_$6_$3.txt
#DW jobdw capacity=1289GiB access_mode=striped type=scratch pool=sm_pool
echo "IO_DRIVER:STAGE"
echo "IO_MODE:$6"
echo "N_NODE:$2"
echo "N_PROC:$3"
mkdir -p $1
rm -rf $1/stage_$6_$3
mkdir $1/stage_$6_$3
export MPICH_MPIIO_HINTS="*:romio_cb_write=disable"
mkdir ${DW_JOB_STRIPED}stage
echo ${DW_JOB_STRIPED}stage
srun -n $3 --export=MPICH_MPIIO_HINTS ./flash_benchmark_io ${DW_JOB_STRIPED}stage/flash_ 
echo "./stageout "${DW_JOB_STRIPED}"stage "$1"/stage_$6_$3"
srun -n 1 ./stageout ${DW_JOB_STRIPED}stage $1/stage_$6_$3
echo "BB Info: "
module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $5}')
echo "session ID is: "${sessID}
instID=$(dwstat instances | grep $sessID | awk '{print $5}')
echo "instance ID is: "${instID}
echo "fragments list:"
echo "frag state instID capacity gran node"
dwstat fragments | grep ${instID}
])dnl
SCRIPT(EXPDIR,NNODE,NPROC,LVL,[$1],IOMODE)

