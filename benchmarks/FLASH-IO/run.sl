#SBATCH -p debug
#SBATCH -N 1 
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH -o flash_1.txt
#SBATCH -L scratch
#DW jobdw capacity=1289GiB access_mode=striped type=scratch pool=sm_pool

RUNS=(1) # Number of runs
OUTDIR=/global/cscratch1/sd/khl7265/FS_64_8M/flash
BBDIR=${DW_JOB_STRIPED}flash
NN=${SLURM_NNODES}
let NP=NN*8
#let NP=NN*32

echo "mkdir -p ${OUTDIR}"
mkdir -p ${OUTDIR}
echo "mkdir -p ${BBDIR}"
mkdir -p ${BBDIR}

for i in ${RUNS[@]}
do
    for u in blocking nonblocking
    do
        for v in coll indep
        do
            # Ncmpi
            if [ "x${v}" = "xcoll" ]; then
                echo "rm -f ${OUTDIR}/*"
                rm -f ${OUTDIR}/*
                
                srun -n ${NP} ./flash_benchmark_io ${OUTDIR}/flash_ ${u} ${v}

                echo "#%$: io_driver: ncmpi"
                echo "#%$: number_of_nodes: ${NN}"
                echo "#%$: io_mode: ${u}_${v}"

                echo "ls -lah ${OUTDIR}"
                ls -lah ${OUTDIR}
                
                echo '-----+-----++------------+++++++++--+---'
            fi

            # Dw
            if [ "x${u}" = "xblocking" ] && [ "x${v}" = "xcoll" ]; then
                export PNETCDF_HINTS="nc_dw_driver=enable;nc_dw_del_on_close=disable;nc_dw_overwrite=enable;nc_dw_dirname=${BBDIR}"

                echo "rm -f ${OUTDIR}/*"
                rm -f ${OUTDIR}/*
                echo "rm -f ${BBDIR}/*"
                rm -f ${BBDIR}/*
                
                srun -n ${NP} ./flash_benchmark_io ${OUTDIR}/flash_ ${u} ${v}

                echo "#%$: io_driver: dw"
                echo "#%$: number_of_nodes: ${NN}"
                echo "#%$: io_mode: ${u}_${v}"

                echo "ls -lah ${OUTDIR}"
                ls -lah ${OUTDIR}
                echo "ls -lah ${BBDIR}"
                ls -lah ${BBDIR}

                unset PNETCDF_HINTS
                            
                echo '-----+-----++------------+++++++++--+---'
            fi
            
            # Dw shared
            if [ "x${u}" = "xblocking" ] && [ "x${v}" = "xcoll" ]; then
                export PNETCDF_HINTS="nc_dw_driver=enable;nc_dw_del_on_close=disable;nc_dw_overwrite=enable;nc_dw_sharedlog=enable;nc_dw_dirname=${BBDIR}"

                echo "rm -f ${OUTDIR}/*"
                rm -f ${OUTDIR}/*
                echo "rm -f ${BBDIR}/*"
                rm -f ${BBDIR}/*
                
                srun -n ${NP} ./flash_benchmark_io ${OUTDIR}/flash_ ${u} ${v}

                echo "#%$: io_driver: dw_shared"
                echo "#%$: number_of_nodes: ${NN}"
                echo "#%$: io_mode: ${u}_${v}"

                echo "ls -lah ${OUTDIR}"
                ls -lah ${OUTDIR}
                                
                unset PNETCDF_HINTS
                
                echo '-----+-----++------------+++++++++--+---'
            fi

            # Staging
            if [ "x${u}" = "xblocking" ] && [ "x${v}" = "xcoll" ]; then
                export stageout_bb_path="${BBDIR}"
                export stageout_pfs_path="${OUTDIR}"
            fi

            echo "rm -f ${OUTDIR}/*"
            rm -f ${OUTDIR}/*
            echo "rm -f ${BBDIR}/*"
            rm -f ${BBDIR}/*
            
            srun -n ${NP} ./flash_benchmark_io ${BBDIR}/flash_ ${u} ${v}

            echo "#%$: io_driver: stage"
            echo "#%$: number_of_nodes: ${NN}"
            echo "#%$: io_mode: ${u}_${v}"

            echo "ls -lah ${OUTDIR}"
            ls -lah ${OUTDIR}
            echo "ls -lah ${BBDIR}"
            ls -lah ${BBDIR}
            
            if [ "x${u}" = "xblocking" ] && [ "x${v}" = "xcoll" ]; then
                unset stageout_bb_path
                unset stageout_pfs_path
            fi

            echo '-----+-----++------------+++++++++--+---'
        done
    done
done

echo "BB Info: "
module load dws
sessID=$(dwstat sessions | grep $SLURM_JOBID | awk '{print $1}')
echo "session ID is: "${sessID}
instID=$(dwstat instances | grep $sessID | awk '{print $1}')
echo "instance ID is: "${instID}
echo "fragments list:"
echo "frag state instID capacity gran node"
dwstat fragments | grep ${instID}


