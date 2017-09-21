      program flash_benchmark_io
!
! This is a sample program that setups the FLASH data structures and 
! drives the I/O routines.  It is intended for benchmarking the I/O
! performance.
! 

! #ifndef MPI_OFFSET
! #define MPI_OFFSET MPI_INTEGER8
! #endif

#ifdef NAGFortran
      USE F90_UNIX_ENV, only : iargc, getarg
#endif

! the main data structures are contained in common blocks, defined in the
! include files

#include "common.fh"

#ifndef NAGFortran
      integer iargc
#endif
      integer i, argc, ierr
      character(len=128) executable
      logical verbose, isArgvRight

      double precision time_io(3), time_begin
      double precision chk_io, corner_io, nocorner_io
      double precision checkpoint_wr_ncmpi_par
      double precision plotfile_ncmpi_par

      integer, parameter :: local_blocks = INT(0.8*maxblocks)

! initialize MPI and get the rank and size
      call MPI_INIT(ierr)
      call MPI_Comm_Rank (MPI_Comm_World, MyPE, ierr)
      call MPI_Comm_Size (MPI_Comm_World, NumPEs, ierr)

      MasterPE = 0
      verbose = .TRUE.

      ! root process reads command-line arguments
      if (MyPE .EQ. MasterPE) then
         isArgvRight = .TRUE.
         argc = IARGC()   ! IARGC() does not count the executable name
         call getarg(0, executable)
         if (argc .GT. 2) then
            print *, &
            'Usage: ',trim(executable),' [-q] <ouput file base name>'
            isArgvRight = .FALSE.
         else
            ! default file name prefix
            basenm = "flash_io_test_"
            if (argc .EQ. 1) then
               call getarg(1, basenm)
            else if (argc .EQ. 2) then
               verbose = .FALSE.
               call getarg(2, basenm)
            endif
         endif
      endif

      ! broadcast if command-line arguments are valid
      call MPI_Bcast(isArgvRight, 1, MPI_LOGICAL, MasterPE, &
                     MPI_COMM_WORLD, ierr)
      if (.NOT. isArgvRight) goto 999

      ! broadcast file base name prefix
      call MPI_Bcast(basenm, 128, MPI_CHARACTER, MasterPE, &
                     MPI_COMM_WORLD, ierr)

! put ~100 blocks on each processor -- make it vary a little, since it does
! in the real application.  This is the maximum that we can fit on Blue 
! Pacific comfortably.
      lnblocks = local_blocks + mod(MyPE,3)

! just fill the tree stucture with dummy information -- we are just going to
! dump it out
      size(:,:) = 0.5e0
      lrefine(:) = 1
      nodetype(:) = 1
      refine(:) = .FALSE.
      derefine(:) = .FALSE.
      parent(:,:) = -1
      child(:,:,:) = -1
      coord(:,:) = 0.25e0
      bnd_box(:,:,:) = 0.e0
      neigh(:,:,:) = -1
      empty(:) = 0

      ! use nonblocking APIs
      ! use_nonblocking_io = .TRUE.
      use_nonblocking_io = .FALSE.

! initialize the unknowns with the index of the variable
      do i = 1, nvar
        unk(i,:,:,:,:) = float(i)
      enddo

      bb_api = 0
      bb_put = 0
      bb_wr = 0
      bb_flush = 0
      bb_rd = 0
      bb_replay = 0
      bb_data  = 0
      bb_meta = 0
      bb_buffer = 0

!---------------------------------------------------------------------------
! netCDF checkpoint file
!---------------------------------------------------------------------------
      time_begin = MPI_Wtime()
      chk_io = checkpoint_wr_ncmpi_par(0,0.e0)
      time_io(1) = MPI_Wtime() - time_begin

!---------------------------------------------------------------------------
! netCDF plotfile -- no corners
!---------------------------------------------------------------------------
      time_begin = MPI_Wtime()
      nocorner_io = plotfile_ncmpi_par(0,0.e0,.false.)
      time_io(2) = MPI_Wtime() - time_begin
    
!---------------------------------------------------------------------------
! netCDF plotfile -- corners
!---------------------------------------------------------------------------
      time_begin = MPI_Wtime()
      corner_io = plotfile_ncmpi_par(0,0.e0,.true.)
      time_io(3) = MPI_Wtime() - time_begin
    
      call report_io_performance(verbose, local_blocks, time_io, chk_io, &
                                 corner_io, nocorner_io)

 999  call MPI_Finalize(ierr)

      end program flash_benchmark_io

! ---------------------------------------------------------------------------
! print mpi info
! ---------------------------------------------------------------------------
      subroutine print_info(info_used)
          use pnetcdf
          
          implicit none
          include "mpif.h"

          integer info_used

          ! local variables
          character*(MPI_MAX_INFO_VAL) key, value
          integer nkeys, i, err
          logical flag

          call MPI_Info_get_nkeys(info_used, nkeys, err)
          print *, 'MPI File Info: nkeys =', nkeys
          do i=0, nkeys-1
              call MPI_Info_get_nthkey(info_used, i, key, err)
              call MPI_Info_get(info_used, key, MPI_MAX_INFO_VAL, value, flag, err)
 123          format('MPI File Info: [',I2,'] key = ',A25, ', value =',A32)
              print 123, i, key, value
          enddo
          print *

          return
      end ! subroutine print_info


! ---------------------------------------------------------------------------
! get the file striping information from the MPI info objects
! ---------------------------------------------------------------------------
      subroutine get_file_striping(info, striping_factor, striping_unit)
          implicit none
          include 'mpif.h'
          integer, intent(in)  :: info
          integer, intent(out) :: striping_factor
          integer, intent(out) :: striping_unit

          ! local variables
          character*(MPI_MAX_INFO_VAL) key, value
          integer                      i, nkeys, valuelen, ierr
          logical                      flag

          call MPI_Info_get_nkeys(info, nkeys, ierr)
          do i=0, nkeys-1
              key(:) = ' '
              call MPI_Info_get_nthkey(info, i, key, ierr)
              call MPI_Info_get(info, key, MPI_MAX_INFO_VAL, value, &
                                flag, ierr)
              call MPI_Info_get_valuelen(info, key, valuelen, flag, &
                                ierr)
              value(valuelen+1:) = ' '
              if (key(len_trim(key):len_trim(key)) .EQ. char(0)) &
                  key(len_trim(key):) = ' '
              if (trim(key) .EQ. 'striping_factor') &
                  read(value, '(i10)') striping_factor
              if (trim(key) .EQ. 'striping_unit') &
                  read(value, '(i10)') striping_unit
          enddo
      end subroutine get_file_striping


!---------------------------------------------------------------------------
! print I/O performance numbers
!---------------------------------------------------------------------------
      subroutine report_io_performance(verbose, local_blocks, time_io, &
                                       chk_io, corner_io, nocorner_io)
       use pnetcdf
#include "common.fh"

       logical verbose
       integer local_blocks
       double precision time_io(3)
       double precision chk_io, corner_io, nocorner_io

       ! local variables
       integer ierr, striping_factor, striping_unit, MaxPE
       double precision tmax(3), ttotal(2), time_total, io_amount, bw
       integer(kind=MPI_OFFSET_KIND) malloc_size, sum_size

       ttotal(1) = time_io(1) + time_io(2) + time_io(3)
       ttotal(2) = MyPE
       call MPI_Allreduce(ttotal, tmax, 2, MPI_2DOUBLE_PRECISION, MPI_MAXLOC, MPI_COMM_WORLD, ierr)
       MaxPE = tmax(2)

       call MPI_Reduce(chk_t, tmax, 3, MPI_DOUBLE_PRECISION, MPI_MAX, &
                       MaxPE, MPI_COMM_WORLD, ierr)
       chk_t(:) = tmax(:)

       call MPI_Reduce(corner_t, tmax, 3, MPI_DOUBLE_PRECISION, MPI_MAX, &
                       MaxPE, MPI_COMM_WORLD, ierr)
       corner_t(:) = tmax(:)

       call MPI_Reduce(nocorner_t, tmax, 3, MPI_DOUBLE_PRECISION, MPI_MAX, &
                       MaxPE, MPI_COMM_WORLD, ierr)
       nocorner_t(:) = tmax(:)

       call MPI_Reduce(time_io, tmax, 3, MPI_DOUBLE_PRECISION, MPI_MAX, &
                       MaxPE, MPI_COMM_WORLD, ierr)

       call MPI_Reduce(chk_io, bw, 1, MPI_DOUBLE_PRECISION, MPI_SUM, &
                       MaxPE, MPI_COMM_WORLD, ierr)
       chk_io = bw
       call MPI_Reduce(nocorner_io, bw, 1, MPI_DOUBLE_PRECISION, MPI_SUM, &
                       MaxPE, MPI_COMM_WORLD, ierr)
       nocorner_io = bw
       call MPI_Reduce(corner_io, bw, 1, MPI_DOUBLE_PRECISION, MPI_SUM, &
                       MaxPE, MPI_COMM_WORLD, ierr)
       corner_io = bw

      if (MyPE .EQ. MaxPE) then
          
            call print_info(info_used)

            striping_factor = 0
            striping_unit   = 0
            call get_file_striping(info_used, striping_factor, striping_unit)

            time_total = tmax(1) + tmax(2) + tmax(3)
            io_amount = chk_io + nocorner_io + corner_io
            bw = io_amount / 1048576.0
            io_amount = bw
            bw = bw / time_total

 1001 format('#%$: ', A, ': ', F16.2)
 1002 format('#%$: ', A, ': ', I13)
 1003 format('#%$: ', A, ': ', A)
 1004 format(' -------------------------------------------------------')

            print 1001,' number_of_guards',nguard
            print 1001,' number_of_blocks',local_blocks
            print 1001,' number_of_variables',nvar
            print 1001,' checkpoint_time        ',tmax(1)
            print 1001,' checkpoint_time_header  ',chk_t(1)
            print 1001,' checkpoint_time_other  ',chk_t(2)
            print 1001,' checkpoint_time_close  ',chk_t(3)
            print 1001,' checkpoint_IO_size     ',chk_io/1048576
            print 1001,' plot_no_corner_time         ',tmax(2)
            print 1001,' plot_no_corner_time_header  ',nocorner_t(1)
            print 1001,' plot_no_corner_time_unknown ',nocorner_t(2)
            print 1001,' plot_no_corner_time_close   ',nocorner_t(3)
            print 1001,' plot_no_corner_time_amount  ',nocorner_io/1048576
            print 1001,' plot_corner_time         ',tmax(3)
            print 1001,' plot_corner_time_header ',corner_t(1)
            print 1001,' plot_corner_time_other    ',corner_t(2)
            print 1001,' plot_corner_time_close ',corner_t(3)
            print 1001,' plot_corner_IO_size ',corner_io/1048576
            print 1004
            print 1003,' file_base_name        ', trim(basenm)
            if (striping_factor .GT. 0) then
                  print 1002,'   file_striping_count  ',striping_factor
                  print 1002,'   file_striping_size   ',striping_unit
            endif
            print 1001,' total_io_size       ',io_amount
            print 1002,' number_of_processes', NumPEs
            print 1002,' dim_x', nxb
            print 1002,' dim_y', nyb
            print 1002,' dim_z', nzb
            print 1001,' total_time       ',time_total
            print 1001,' bandwidth       ',bw
      endif
      call MPI_Info_free(info_used, ierr)

      ! print info about PnetCDF internal malloc usage
      ierr = nfmpi_inq_malloc_max_size(malloc_size)
      if (ierr .EQ. NF_NOERR) then
          call MPI_Reduce(malloc_size, sum_size, 1, MPI_OFFSET, MPI_SUM, &
                          MasterPE, MPI_COMM_WORLD, ierr)
          if (verbose .AND. MyPE .EQ. MasterPE) &
              print 1002, &
              'maximum heap memory allocted by PnetCDF internally is', &
              sum_size/1048576, ' MiB'

          ierr = nfmpi_inq_malloc_size(malloc_size)
          call MPI_Reduce(malloc_size, sum_size, 1, MPI_OFFSET, MPI_SUM, &
                          MasterPE, MPI_COMM_WORLD, ierr)
          if (verbose .AND. MyPE .EQ. MasterPE .AND. &
              sum_size .GT. 0_MPI_OFFSET_KIND) &
              print 1002, &
              'heap memory allocated by PnetCDF internally has ', &
              sum_size/1048576, ' MiB yet to be freed'
      endif

      end subroutine report_io_performance




