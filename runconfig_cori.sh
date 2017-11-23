./configure --prefix=/path/to/install \
                cross_compiling="yes" \
                CFLAGS="-fast -no-ipo" CXXFLAGS="-fast -no-ipo" \
                FFLAGS="-fast -no-ipo"  FCFLAGS="-fast -no-ipo" \
                TESTMPIRUN="srun -n NP" TESTSEQRUN="srun -n 1" \
                TESTOUTDIR="$SCRATCH" \
		--disable-shared --disable-debug \
                --enable-profiling --enable-dwdriver \
		--prefix=/global/homes/k/khl7265/local/ncmpi_eval
