define(`SCRIPT', dnl
`dnl
#!/bin/bash
#SBATCH -p regular
#SBATCH -N $2
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -o ncmpi_$4_$3.txt
echo "IO_DRIVER:NCMPIO"
echo "IO_MODE:$4"
echo "N_NODE:$2" 
echo "N_PROC:$3"
cd $PWD
mkdir -p $1
rm -rf $1/ncmpi_$4_$3
mkdir $1/ncmpi_$4_$3
srun -n $3 ./flash_benchmark_io $1/ncmpi_$4_$3/flash_
')dnl
SCRIPT(EXPDIR,NNODE,NPROC,IOMODE)
