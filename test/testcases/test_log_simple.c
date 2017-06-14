#pragma warning(disable : 4996)

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <pnetcdf.h>
#include <unistd.h>
#include <mpi.h>
#include <sys/stat.h>

#define MAXPROCESSES 1024
#define WIDTH 6
#define SINGLEPROCRANK 2
#define SINGLEPROCNP 5

int buffer[MAXPROCESSES];
MPI_Offset start[2], count[2];

/*
The test write a NP * NP matrix M, NP is the number of process:
put_vara:
Process N write N copy of it's rank to row N ([N, 0...WIDTH]) using different APIs on different variable
final result should be:
0 0 0 0 ...
1 1 1 1 ...
2 2 2 2 ...
.
.
.
*/

int main(int argc, char* argv[]) {
	int i, j, ret, errlen;
	int NProc, MyRank, NP;      // Total process; Rank
	int fid;        // Data set ID
	int did[2];     // IDs of dimension
	int vid;        // IDs for variables
	int dims[2];
	char tmp[1024], tmp2[1024];
	struct stat filestat;
	MPI_Info Info;

	if (argc < 2) {
		printf("PTester.exe <output>\n");
		return 0;
	}

	MPI_Init(&argc, &argv);

	MPI_Comm_size(MPI_COMM_WORLD, &NP);
	MPI_Comm_rank(MPI_COMM_WORLD, &MyRank);

	if (NP == 1) {	// Act if there is WIDTH processes for easy debugging. Most debugger supports only single proccesses.
		NProc = SINGLEPROCNP;
		MyRank = SINGLEPROCRANK;
	}
	else{
		NProc = NP;
	}

	if (MyRank < MAXPROCESSES) {
		// Ensure each process have a independent buffer directory

		MPI_Info_create(&Info);
		MPI_Info_set(Info, "pnetcdf_log", "enable");
		MPI_Info_set(Info, "pnetcdf_log_keep", "disable");

		// Create new cdf file
		ret = ncmpi_create(MPI_COMM_WORLD, argv[1], NC_CLOBBER, Info, &fid);
		if (ret != NC_NOERR) {
			printf("Error create file\n");
			goto ERROR;
		}
		ret = ncmpi_set_fill(fid, NC_FILL, NULL);
		if (ret != NC_NOERR) {
			printf("Error set fill\n");
			goto ERROR;
		}
		ret = ncmpi_def_dim(fid, "X", NProc, did);  // X
		if (ret != NC_NOERR) {
			printf("Error def dim X\n");
			goto ERROR;
		}
		ret = ncmpi_def_dim(fid, "Y", NProc, did + 1);	// Y
		if (ret != NC_NOERR) {
			printf("Error def dim Y\n");
			goto ERROR;
		}
		ret = ncmpi_def_var(fid, "M", NC_INT, 2, did, vid);
		if (ret != NC_NOERR) {
			printf("Error def var M\n");
			goto ERROR;
		}
		ret = ncmpi_enddef(fid);
		if (ret != NC_NOERR) {
			printf("Error enddef\n");
			goto ERROR;
		}

		// Indep mode
		ret = ncmpi_begin_indep_data(fid);
		if (ret != NC_NOERR) {
			printf("Error begin indep\n");
			goto ERROR;
		}
	
		if (MyRank == 0 || NP == 1) {
			printf("Size before write:\n");
			system("ls -lh *.{cdf,bin}");
	
			printf("Press any key to start writing.");
			getchar();
		}

		// We all write rank from now on
		for (i = 0; i < NProc; i++) {
			buffer[i] = MyRank;
		}

		// put_vara
		count[0] = 1;
		count[1] = NProc;
		start[0] = MyRank;
		start[1] = 0;
		ret = ncmpi_put_vara_int(fid, vid, start, count, buffer);
		if (ret != NC_NOERR) {
			MPI_Error_string(ret, tmp, &errlen);
			printf("Error put_varn: %d\n%s\n", errlen, tmp);
			goto ERROR;
		}

		if (MyRank == 0 || NP == 1) {
			printf("Size before flush:\n");
			system("ls -lh *.{cdf,bin}");

			printf("Press any key to flush. ");
			getchar();
		}
		MPI_Barrier(MPI_COMM_WORLD);

		// Collective mode
		ncmpi_end_indep_data(fid);
		if (ret != NC_NOERR) {
			printf("Error end indep");
			goto ERROR;
		}

		ncmpi_close(fid);       // Close file
		if (ret != NC_NOERR) {
			printf("Error close");
			goto ERROR;
		}
		
		if (MyRank == 0 || NP == 1) {
			printf("Size after flush:\n");
			system("ls -lh *.{cdf,bin}");
		}

		//printf("Size after replay:\n");
		//system("ls *.{cdf,logfs,logfslock,data,meta} -lah");
	
		/*
		ret = stat(argv[1], &filestat);
		if(ret < 0){
			printf("Error reading file size: %d", ret);
			goto ERROR;
		}
		printf("Size after replay: %lld\n", filestat.st_size);
		*/
	}

ERROR:;
	MPI_Finalize();
	return 0;
}
