define(`SCRIPT', dnl
`dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2
#SBATCH -C haswell
#SBATCH -t 00:20:00
#SBATCH -o log_$3.txt
#DW jobdw capacity=1024GB access_mode=private type=scratch pool=sm_pool
rm -rf $1
mkdir $1
mkdir $1/log
export PNETCDF_HINTS="pnetcdf_bb=enable;pnetcdf_bb_del_on_close=disable;pnetcdf_bb_flush_buffer_size=67108864;pnetcdf_bb_overwrite=enable;pnetcdf_bb_dirname=${DW_JOB_STRIPED}bb"
mkdir ${DW_JOB_STRIPED}bb
echo ${DW_JOB_STRIPED}bb
srun -n $3 ./flash_benchmark_io $1/log/flash_
#cp -r ${DW_JOB_STRIPED}bb $1 
lfs getstripe $1/log
')dnl
SCRIPT(EXPDIR,NNODE,NPROC)
