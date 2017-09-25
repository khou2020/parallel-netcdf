changequote(`[', `]')dnl
define([SCRIPT], dnl
[dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2
#SBATCH -C haswell
#SBATCH -t 00:20:00
#SBATCH -o bb_$5_$6_$3.txt
#DW jobdw capacity=1289GiB access_mode=striped type=scratch pool=sm_pool

export PNETCDF_HINTS="pnetcdf_bb=enable;pnetcdf_bb_del_on_close=disable;pnetcdf_bb_overwrite=enable;pnetcdf_bb_dirname=${DW_JOB_STRIPED}/bb"
echo "BB_Path: "${DW_JOB_STRIPED}bb
mkdir -p $1
mkdir -p $1/bb_$5_$6_$3
mkdir -p ${DW_JOB_STRIPED}bb
for i in 1 2 3
do
    echo "Round " $i ":"
    rm -f $1/bb_$5_$6_$3/*
    rm -f ${DW_JOB_STRIPED}bb/*
    srun -n $3 ./flash_benchmark_io $1/bb_$5_$6_$3/flash_
    echo "#%$: n_nodes: $2"
    echo "#%$: io_driver: bb"
    echo "ls -lah $1/bb_$6_$3/*"
    ls -lah $1/bb_$6_$3/*
    echo '-----+-----++------------+++++++++--+---'
done
echo '--++---+----+++-----++++---+++--+-++--+---'
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
SCRIPT(EXPDIR,NNODE,NPROC,[$1],IOMODEA,IOMDOEB)
