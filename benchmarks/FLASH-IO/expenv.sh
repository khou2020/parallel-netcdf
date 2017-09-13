#!/bin/bash
#WD=$4
module load datawarp 
cc stageout.c -o stageout `pkg-config --cflags --libs cray-datawarp`
./genscript.sh 1 32 5
./genscript.sh 2 64 6
./genscript.sh 4 128 7
./genscript.sh 8 256 7
./genscript.sh 16 512 8
./genscript.sh 32 1024 8
