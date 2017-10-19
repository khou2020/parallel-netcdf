#
# Copyright (C) 2012, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#
# $Id$
#
# @configure_input@

AM_CPPFLAGS  = -I$(top_srcdir)/src/include
AM_CPPFLAGS += -I$(top_builddir)/src/include

bin_PROGRAMS = ncmpilogdump
nodist_ncmpilogdump_SOURCES = ncmpilogdump.c
ncmpilogdump_LDADD = $(top_builddir)/src/libs/libpnetcdf.la
M4FLAGS += -I${top_srcdir}/m4
M4_SRCS  = ncmpilogdump.m4
SUFFIXES = .o .c .h .m4

EXTRA_DIST = $(M4_SRCS)

$(M4_SRCS:.m4=.c): Makefile

.m4.c:
	$(M4) $(AM_M4FLAGS) $(M4FLAGS) $< >$@

$(top_builddir)/src/libs/libpnetcdf.la:
	set -e; cd $(top_builddir)/src/libs && $(MAKE) $(MFLAGS)

tests-local: all

dist-hook:
	$(SED) -i "s/PNETCDF_RELEASE_VERSION/$(PNETCDF_VERSION)/" $(distdir)/ncmpilogdump.1
	$(SED) -i "s/PNETCDF_RELEASE_DATE/`date '+%e %b %Y'`/"    $(distdir)/ncmpilogdump.1
	$(SED) -i "s/PNETCDF_RELEASE_DATE2/`date '+%Y-%m-%d'`/"   $(distdir)/ncmpilogdump.1

BUILT_SOURCES = $(M4_SRCS:.m4=.c)
CLEANFILES = $(M4_SRCS:.m4=.c) *.gcno
