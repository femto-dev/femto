AM_CPPFLAGS = -I$(srcdir)/../utils -I$(srcdir)/../utils_cc -I$(srcdir)/../mpi -I$(srcdir)/../main
CC=$(MPICC)
CXX=$(MPICXX)

noinst_LTLIBRARIES = libfemto_dcx_cc.la

libfemto_dcx_cc_la_SOURCES = dcx.hh dcx.cc dcover.hh dcover.cc dcx_inmem.hh dcx_inmem.cc two_stage.hh two_stage.cc

LDADD = libfemto_dcx_cc.la ../main/libfemto_seq.la ../main/libfemto.la ../main/libfemto_bwt.la ../mpi/libfemto_mpi_cc.la ../utils_cc/libfemto_util_cc.la ../utils/libfemto_util.la -lssl -lcrypto

check_SCRIPTS = suffix_sort_test.sh
EXTRA_DIST = suffix_sort_test.sh CMakeLists.txt

check_PROGRAMS = dcover_test suffix_sort_test suffix_sort_inmem_test

bin_PROGRAMS = femto_suffix_sort femto_index femto_scan femto_size femto_index_fasta femto_scan_fasta femto_size_fasta femto_index_zero femto_index_zero femto_scan_zero femto_size_zero

dcover_test_SOURCES = dcover_test.cc
suffix_sort_test_SOURCES = suffix_sort_test.cc
suffix_sort_inmem_test_SOURCES = suffix_sort_inmem_test.cc
femto_suffix_sort_SOURCES = suffix_sort_tool.cc
femto_index_SOURCES = index_tool.cc index_tool_support_files.c index_tool_support.h
femto_scan_SOURCES = scan_tool.cc index_tool_support_files.c index_tool_support.h
femto_scan_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
femto_scan_CPPFLAGS = -I $(srcdir)/../re2/re2/ $(AM_CPPFLAGS)
femto_size_SOURCES = size_tool.cc index_tool_support_files.c index_tool_support.h

femto_index_fasta_SOURCES = index_tool.cc index_tool_support_fasta.c index_tool_support.h
femto_scan_fasta_SOURCES = scan_tool.cc index_tool_support_fasta.c index_tool_support.h
femto_scan_fasta_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
femto_scan_fasta_CPPFLAGS = -I $(srcdir)/../re2/re2 $(AM_CPPFLAGS)

# size tool will approximate based on file size for FASTA
# don't want to read all of the data.
femto_size_fasta_SOURCES = size_tool.cc index_tool_support_files.c index_tool_support.h

femto_index_zero_SOURCES = index_tool.cc index_tool_support_zero.c index_tool_support.h
femto_scan_zero_SOURCES = scan_tool.cc index_tool_support_zero.c index_tool_support.h
femto_scan_zero_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
femto_scan_zero_CPPFLAGS = -I $(srcdir)/../re2/re2 $(AM_CPPFLAGS)
femto_size_zero_SOURCES = size_tool.cc index_tool_support_files.c index_tool_support.h

TESTS = $(check_PROGRAMS) $(check_SCRIPTS)

