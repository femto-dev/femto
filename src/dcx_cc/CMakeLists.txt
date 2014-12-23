# include ../util
include_directories(${CMAKE_SOURCE_DIR}/src/utils)
# include ../util_cc
include_directories(${CMAKE_SOURCE_DIR}/src/utils_cc)
# include ../main
include_directories(${CMAKE_SOURCE_DIR}/src/main)
# include ../mpi
include_directories(${CMAKE_SOURCE_DIR}/src/mpi)
# include ../re2
include_directories(${CMAKE_SOURCE_DIR}/src/re2/re2/)

set(FEMTO_DCX_CC_SRCS
    dcx.hh
    dcx.cc
    dcover.hh
    dcover.cc
    dcover_internal.h
    dcover_internal.c
    dcx_inmem.cc
    two_stage.cc
    )

add_library(femto_dcx_cc STATIC ${FEMTO_DCX_CC_SRCS})

set(DCX_LIBS femto_dcx_cc femto_seq femto femto_bwt femto_mpi_cc femto_util_cc femto_util ssl crypto ${DEFAULT_LIBRARIES})

if(HAS_LINUX_LIBAIO)
  set(DCX_LIBS "${DCX_LIBS} -laio")
endif()


set(DCX_TESTS dcover_test suffix_sort_test suffix_sort_inmem_test)

foreach(test ${DCX_TESTS})
  add_executable(${test} EXCLUDE_FROM_ALL ${test}.cc)
  add_test(${test} ${test})
  target_link_libraries(${test} ${DCX_LIBS})
  add_dependencies(check ${test})
endforeach(test)



add_executable(femto_suffix_sort suffix_sort_tool.cc)
target_link_libraries(femto_suffix_sort ${DCX_LIBS})
install(TARGETS femto_suffix_sort ${INSTALL_TARGETS_DEFAULT_ARGS})

set(femto_index_SRCS index_tool.cc index_tool_support_files.c index_tool_support.h)
add_executable(femto_index ${femto_index_SRCS})
target_link_libraries(femto_index ${DCX_LIBS})
install(TARGETS femto_index ${INSTALL_TARGETS_DEFAULT_ARGS})


set(femto_scan_SRCS scan_tool.cc index_tool_support_files.c index_tool_support.h)
add_executable(femto_scan ${femto_scan_SRCS})
target_link_libraries(femto_scan ${DCX_LIBS} ${RE2_LIB})
install(TARGETS femto_scan ${INSTALL_TARGETS_DEFAULT_ARGS})


set(femto_size_SRCS size_tool.cc index_tool_support_files.c index_tool_support.h)
add_executable(femto_size ${femto_size_SRCS})
target_link_libraries(femto_size ${DCX_LIBS})
install(TARGETS femto_size ${INSTALL_TARGETS_DEFAULT_ARGS})

set(femto_index_fasta_SRCS index_tool.cc index_tool_support_fasta.c index_tool_support.h)
add_executable(femto_index_fasta ${femto_index_fasta_SRCS})
target_link_libraries(femto_index_fasta ${DCX_LIBS})
install(TARGETS femto_index_fasta ${INSTALL_TARGETS_DEFAULT_ARGS})

set(femto_scan_fasta_SRCS scan_tool.cc index_tool_support_fasta.c index_tool_support.h)
add_executable(femto_scan_fasta ${femto_scan_fasta_SRCS})
target_link_libraries(femto_scan_fasta ${DCX_LIBS} ${RE2_LIB})
install(TARGETS femto_scan_fasta ${INSTALL_TARGETS_DEFAULT_ARGS})


set(femto_size_fasta_SRCS size_tool.cc index_tool_support_files.c index_tool_support.h)
add_executable(femto_size_fasta ${femto_size_fasta_SRCS})
target_link_libraries(femto_size_fasta ${DCX_LIBS})
install(TARGETS femto_size_fasta ${INSTALL_TARGETS_DEFAULT_ARGS})

set(femto_index_zero_SRCS index_tool.cc index_tool_support_zero.c index_tool_support.h)
add_executable(femto_index_zero ${femto_index_zero_SRCS})
target_link_libraries(femto_index_zero ${DCX_LIBS})
install(TARGETS femto_index_zero ${INSTALL_TARGETS_DEFAULT_ARGS})


set(femto_scan_zero_SRCS scan_tool.cc index_tool_support_zero.c index_tool_support.h)
add_executable(femto_scan_zero ${femto_scan_zero_SRCS})
target_link_libraries(femto_scan_zero ${DCX_LIBS} ${RE2_LIB})
install(TARGETS femto_scan_zero ${INSTALL_TARGETS_DEFAULT_ARGS})


set(femto_size_zero_SRCS size_tool.cc index_tool_support_files.c index_tool_support.h)
add_executable(femto_size_zero ${femto_size_zero_SRCS})
target_link_libraries(femto_size_zero ${DCX_LIBS})
install(TARGETS femto_size_zero ${INSTALL_TARGETS_DEFAULT_ARGS})

configure_file(suffix_sort_test.sh suffix_sort_test.sh COPYONLY)

#original Makefile.am contents follow:

#AM_CPPFLAGS = -I$(srcdir)/../utils -I$(srcdir)/../utils_cc -I$(srcdir)/../mpi -I$(srcdir)/../main
#CC=$(MPICC)
#CXX=$(MPICXX)
#
#noinst_LTLIBRARIES = libfemto_dcx_cc.la
#
#libfemto_dcx_cc_la_SOURCES = dcx.hh dcx.cc dcover.hh dcover.cc
#
#LDADD = libfemto_dcx_cc.la ../main/libfemto_seq.la ../main/libfemto.la ../main/libfemto_bwt.la ../mpi/libfemto_mpi_cc.la ../utils_cc/libfemto_util_cc.la ../utils/libfemto_util.la -lssl -lcrypto
#
#check_SCRIPTS = suffix_sort_test.sh
#EXTRA_DIST = suffix_sort_test.sh
#
#check_PROGRAMS = dcover_test suffix_sort_test 
#
#bin_PROGRAMS = femto_suffix_sort femto_index femto_scan femto_size femto_index_fasta femto_scan_fasta femto_size_fasta femto_index_zero femto_index_zero femto_scan_zero femto_size_zero
#
#dcover_test_SOURCES = dcover_test.cc
#suffix_sort_test_SOURCES = suffix_sort_test.cc
#femto_suffix_sort_SOURCES = suffix_sort_tool.cc
#femto_index_SOURCES = index_tool.cc index_tool_support_files.c index_tool_support.h
#femto_scan_SOURCES = scan_tool.cc index_tool_support_files.c index_tool_support.h
#femto_scan_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
#femto_scan_CPPFLAGS = -I $(srcdir)/../re2/re2/ $(AM_CPPFLAGS)
#femto_size_SOURCES = size_tool.cc index_tool_support_files.c index_tool_support.h
#
#femto_index_fasta_SOURCES = index_tool.cc index_tool_support_fasta.c index_tool_support.h
#femto_scan_fasta_SOURCES = scan_tool.cc index_tool_support_fasta.c index_tool_support.h
#femto_scan_fasta_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
#femto_scan_fasta_CPPFLAGS = -I $(srcdir)/../re2/re2 $(AM_CPPFLAGS)
#
## size tool will approximate based on file size for FASTA
## don't want to read all of the data.
#femto_size_fasta_SOURCES = size_tool.cc index_tool_support_files.c index_tool_support.h
#
#femto_index_zero_SOURCES = index_tool.cc index_tool_support_zero.c index_tool_support.h
#femto_scan_zero_SOURCES = scan_tool.cc index_tool_support_zero.c index_tool_support.h
#femto_scan_zero_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
#femto_scan_zero_CPPFLAGS = -I $(srcdir)/../re2/re2 $(AM_CPPFLAGS)
#femto_size_zero_SOURCES = size_tool.cc index_tool_support_files.c index_tool_support.h
#
#TESTS = $(check_PROGRAMS) $(check_SCRIPTS)
#
