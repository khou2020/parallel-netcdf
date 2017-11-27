#
# Copyright (C) 2012, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#
# $Id$
#
# @configure_input@

AM_CPPFLAGS  = -I$(top_srcdir)/src/include
AM_CPPFLAGS += -I$(top_builddir)/src/include

bin_PROGRAMS = ncmpizip
nodist_ncmpizip_SOURCES =   common.h \
					    	common.c \
							ncmpizip.c \
							zip.c \
							unzip.c

ncmpizip_LDADD = $(top_builddir)/src/libs/libpnetcdf.la

SUFFIXES = .o .c .h

$(top_builddir)/src/libs/libpnetcdf.la:
	set -e; cd $(top_builddir)/src/libs && $(MAKE) $(MFLAGS)

tests-local: all

CLEANFILES = *.gcno
