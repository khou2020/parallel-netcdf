#pragma warning(disable : 4996)

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <pnetcdf.h>
#include <unistd.h>
#include <mpi.h>

#define MAXPROCESSES 1024
#define WIDTH 6
#define SINGLEPROCRANK 2
#define SINGLEPROCNP 5

int buffer[MAXPROCESSES];
MPI_Offset start[MAXPROCESSES][2], count[MAXPROCESSES][2];
MPI_Offset *sp[MAXPROCESSES], *cp[MAXPROCESSES];
MPI_Offset stride[2];


/*
The demo write a NP * (NP * 4) matrix M, which is divided into 4 NP * WIDTH submatrixes, NP is the number of process:
There are 4 submatrix for demonstrating put_var1, put_vara, put_vars, put_varn, put_var is used in each variable to set it to 0 in the begining
The same pattern is writen 4 time to 4 variables with the position of submatrix swaped:
0: put_var1, put_vara, put_vars, put_varn
1: put_vara, put_vars, put_varn, put_var1
2: put_vars, put_varn, put_var1, put_vara
3: put_varn, put_var1, put_vara, put_vars

put_var1:
Process N writeit's rank to [N, 0] leaving other cell unattained
final result should be:
0 0 0 0 ...
1 0 0 0 ...
2 0 0 0 ...
.
.
.
put_vara:
Process N write N copy of it's rank to row N ([N, 0...WIDTH]) using different APIs on different variable
final result should be:
0 0 0 0 ...
1 1 1 1 ...
2 2 2 2 ...
.
.
.
put_varas:
Process N write N copy of it's rank to every 2 columns on row N ([N, 0 2 4 ... WIDTH]) using different APIs on different variable
final result should be:
0 0 0 0 ...
1 0 1 0 ...
2 0 2 0 ...
.
.
.
put_varan:
Process N write N copy of it's rank to diagonal starts on row N ([(N ... N + WIDTH) % NP, 0 ... WIDTH]) using different APIs on different variable
final result should be:
0 0 0 0 ...
1 0 0 0 ...
2 1 0 0 ...
3 2 1 0 ...
.
.
*/
int main(int argc, char* argv[]) {
	int i, j, ret;
	int NProc, MyRank, NP;      // Total process; Rank
	int fid;        // Data set ID
	int did[2];     // IDs of dimension
	int vid[4];        // IDs for variables
	int dims[2];
	char tmp[1024];
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
		MPI_Info_set(Info, "pnetcdf_log_keep", "disable");	/* Do not delete after flush */

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
		ret = ncmpi_def_dim(fid, "Y", NProc * 4, did + 1);	// Y
		if (ret != NC_NOERR) {
			printf("Error def dim Y\n");
			goto ERROR;
		}
		ret = ncmpi_def_var(fid, "M0", NC_INT, 2, did, vid + 0);
		if (ret != NC_NOERR) {
			printf("Error def var M0\n");
			goto ERROR;
		}
		ret = ncmpi_def_var(fid, "M1", NC_INT, 2, did, vid + 1);
		if (ret != NC_NOERR) {
			printf("Error def var M1\n");
			goto ERROR;
		}
		ret = ncmpi_def_var(fid, "M2", NC_INT, 2, did, vid + 2);
		if (ret != NC_NOERR) {
			printf("Error def var M2\n");
			goto ERROR;
		}
		ret = ncmpi_def_var(fid, "M3", NC_INT, 2, did, vid + 3);
		if (ret != NC_NOERR) {
			printf("Error def var M3\n");
			goto ERROR;
		}
		ret = ncmpi_enddef(fid);
		if (ret != NC_NOERR) {
			printf("Error enddef\n");
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
		
		// put_var1
		for (i = 0; i < 4; i++) {
			for (j = 0; j < NProc; j++) {
				start[0][0] = MyRank;
				start[0][1] = i * NProc + j;
				ret = ncmpi_put_var1_int_all(fid, vid[i], start[0], buffer);
				if (ret != NC_NOERR) {
					printf("Error put_var1\n");
					goto ERROR;
				}
			}
		}
		
		// put_vara
		for (i = 0; i < 4; i++) {
			start[0][0] = 0;
			start[0][1] = ((i + 1) % 4) * NProc + MyRank;
			count[0][0] = NProc;
			count[0][1] = 1;
			ret = ncmpi_put_vara_int_all(fid, vid[i], start[0], count[0], buffer);
			if (ret != NC_NOERR) {
				printf("Error put_vara\n");
				goto ERROR;
			}
		}

		// put_vars
		for (i = 0; i < 4; i++) {
			start[0][0] = MyRank;
			start[0][1] = ((i + 2) % 4) * NProc + (MyRank % 2);
			count[0][0] = 1;
			count[0][1] = NProc / 2;
			stride[0] = 1;
			stride[1] = 2;
			ret = ncmpi_put_vars_int_all(fid, vid[i], start[0], count[0], stride, buffer);
			if (ret != NC_NOERR) {
				printf("Error put_vars\n");
				goto ERROR;
			}
		}

		// put_varn
		for (j = 0; j < 4; j++) {
			for (i = 0; i < NProc; i++) {
				count[i][0] = 1;
				count[i][1] = 1;
				start[i][0] = (MyRank + i) % NProc;
				start[i][1] = i + ((j + 3) % 4) * NProc;
				sp[i] = (MPI_Offset*)start[i];
				cp[i] = (MPI_Offset*)count[i];
			}
			ret = ncmpi_put_varn_int_all(fid, vid[j], NProc, sp, cp, buffer);
			if (ret != NC_NOERR) {
				printf("Error put_varn\n");
				goto ERROR;
			}
		}

		if (MyRank == 0 || NP == 1) {
			printf("Size before flush:\n");
			system("ls -lh *.{cdf,bin}");

			printf("Press any key to flush. ");
			getchar();
		}
		MPI_Barrier(MPI_COMM_WORLD);

		// Commit log into cdf file
		//ncmpi_flush(fid);	// Commit buffer to disk

		ret = ncmpi_close(fid);       // Close file
		if (ret != NC_NOERR) {
			printf("Error close");
			goto ERROR;
		}

		if (MyRank == 0 || NP == 1) {
			printf("Size after flush:\n");
			system("ls -lh *.{cdf,bin}");
		}
	}

ERROR:;
	MPI_Finalize();

	return 0;
}
