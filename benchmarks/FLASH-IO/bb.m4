changequote(`[', `]')dnl
define([SCRIPT], dnl
[dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2
#SBATCH -C haswell
#SBATCH -t 00:20:00
#SBATCH -o bb_$5_$3.txt
#DW jobdw capacity=1289GiB access_mode=striped type=scratch pool=sm_pool
echo "IO_DRIVER:BB"
echo "IO_MODE:$5"
echo "N_NODE:$2"
echo "N_PROC:$3"
export PNETCDF_HINTS="pnetcdf_bb=enable;pnetcdf_bb_del_on_close=disable;pnetcdf_bb_flush_buffer_size=67108864;pnetcdf_bb_overwrite=enable;pnetcdf_bb_dirname=${DW_JOB_STRIPED}/bb"
echo "BB_Path: "${DW_JOB_STRIPED}bb
mkdir -p $1
for i in 1 2 3
do
    echo "Round " $i ":"
    rm -rf $1/bb_$5_$3
    mkdir $1/bb_$5_$3
    rm -rf ${DW_JOB_STRIPED}bb
    mkdir -p ${DW_JOB_STRIPED}bb
    srun -n $3 ./flash_benchmark_io $1/bb_$5_$3/flash_
done
echo "BB Info: "
module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $4}')
echo "session ID is: "${sessID}
instID=$(dwstat instances | grep $sessID | awk '{print $4}')
echo "instance ID is: "${instID}
echo "fragments list:"
echo "frag state instID capacity gran node"
dwstat fragments | grep ${instID}
])dnl
SCRIPT(EXPDIR,NNODE,NPROC,[$1],IOMODE)
