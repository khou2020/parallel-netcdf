#
# Copyright (C) 2017, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#
# $Id$
#
# @configure_input@

SUFFIXES = .a .o .c .m4 .h

AM_CPPFLAGS  = -I${top_srcdir}/src/include
AM_CPPFLAGS += -I${top_builddir}/src/include
AM_CPPFLAGS += -I${top_srcdir}/src/drivers/include
AM_CPPFLAGS += -I${top_srcdir}/src/drivers/ncmpio

if PNETCDF_DEBUG
   AM_CPPFLAGS += -DPNETCDF_DEBUG
endif

noinst_LTLIBRARIES = libnclogio.la

M4FLAGS += -I${top_srcdir}/m4

# M4_HFILES = ncx_h.m4

M4_SRCS   = log_flush.m4 
#            i_getput.m4 \
#            ncx.m4 \
#            varn.m4 \
#            i_varn.m4 \
#            attr.m4 \
#            convert_swap.m4

H_SRCS = log_dispatch.h \
         log.h 

C_SRCS = nclogio_dispatch.c \
		 ncmpio.c \
		 nclogio_open.c \
		 nclogio_create.c \
		 nclogio_sync.c \
		 nclogio_close.c \
		 nclogio_put.c \
		 nclogio_enddef.c \
		 log_flush.c \
		 log_mem.c \
		 log_put.c \
		 error.c
        
$(M4_SRCS:.m4=.c): Makefile

.m4.c:
	$(M4) $(AM_M4FLAGS) $(M4FLAGS) $< >$@

libnclogio_la_SOURCES = $(C_SRCS) $(H_SRCS)
# nodist_libncmpio_la_SOURCES = ncx.h $(M4_SRCS:.m4=.c)

# automake says "... BUILT_SOURCES is honored only by 'make all', 'make check',
# and 'make install'. This means you cannot build a specific target (e.g.,
# 'make foo') in a clean tree if it depends on a built source."
# BUILT_SOURCES = ncx.h $(M4_SRCS:.m4=.c)

CLEANFILES = $(M4_SRCS:.m4=.c) 

EXTRA_DIST = $(M4_SRCS) 

tests-local: all

