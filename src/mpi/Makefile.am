AM_CPPFLAGS = -I$(srcdir)/../utils -I$(srcdir)/../utils_cc
CC=$(MPICC)
CXX=$(MPICXX)

noinst_LTLIBRARIES = libfemto_mpi_cc.la

libfemto_mpi_cc_la_SOURCES = mpi_utils.hh mpi_utils.cc

LDADD = libfemto_mpi_cc.la ../utils_cc/libfemto_util_cc.la ../utils/libfemto_util.la

check_SCRIPTS = mpi_utils_test.sh
EXTRA_DIST = mpi_utils_test.sh

check_PROGRAMS = mpi_utils_test
bin_PROGRAMS = femto_hello_mpi femto_mpi_cp

femto_hello_mpi_SOURCES = hello_mpi.c
femto_mpi_cp_SOURCES = mpi_cp.cc
mpi_utils_test_SOURCES = mpi_utils_test.cc

TESTS = $(check_PROGRAMS) $(check_SCRIPTS)

