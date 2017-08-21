define(`SCRIPT', dnl
`dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -o nolog_$3.txt
rm -rf $1
mkdir $1
mkdir $1/nolog
srun -n $3 ./flash_benchmark_io $1/nolog/flash_
lfs getstripe $1/nolog
')dnl
SCRIPT(EXPDIR,NNODE,NPROC)
