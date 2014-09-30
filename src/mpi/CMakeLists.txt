# include ../util
include_directories(${CMAKE_SOURCE_DIR}/src/utils)
# include ../util_cc
include_directories(${CMAKE_SOURCE_DIR}/src/utils_cc)


set(FEMTO_MPI_CC_SRCS mpi_utils.hh mpi_utils.cc)

add_library(femto_mpi_cc STATIC ${FEMTO_MPI_CC_SRCS})

set(MPI_CC_TESTS mpi_utils_test)

foreach(test ${MPI_CC_TESTS})
  add_executable(${test} EXCLUDE_FROM_ALL ${test}.cc)
  add_test(${test} ${test})
  target_link_libraries(${test} femto_mpi_cc femto_util_cc femto_util ${DEFAULT_LIBRARIES})
  add_dependencies(check ${test})
endforeach(test)

add_executable(femto_hello_mpi hello_mpi.c)

#target_link_libraries(femto_hello_mpi femto_mpi_cc)

install(TARGETS femto_hello_mpi ${INSTALL_TARGETS_DEFAULT_ARGS})

add_executable(femto_mpi_cp mpi_cp.cc)

target_link_libraries(femto_mpi_cp femto_mpi_cc femto_util_cc femto_util ${DEFAULT_LIBRARIES})

install(TARGETS femto_mpi_cp ${INSTALL_TARGETS_DEFAULT_ARGS})


########### install files ###############
configure_file(mpi_utils_test.sh mpi_utils_test.sh COPYONLY)

#install(FILES  mpi_utils_test.sh DESTINATION )



#original Makefile.am contents follow:

#AM_CPPFLAGS = -I$(srcdir)/../utils -I$(srcdir)/../utils_cc
#CC=$(MPICC)
#CXX=$(MPICXX)
#
#noinst_LTLIBRARIES = libfemto_mpi_cc.la
#
#libfemto_mpi_cc_la_SOURCES = mpi_utils.hh mpi_utils.cc
#
#LDADD = libfemto_mpi_cc.la ../utils_cc/libfemto_util_cc.la ../utils/libfemto_util.la
#
#check_SCRIPTS = mpi_utils_test.sh
#EXTRA_DIST = mpi_utils_test.sh
#
#check_PROGRAMS = mpi_utils_test
#bin_PROGRAMS = femto_hello_mpi femto_mpi_cp
#
#femto_hello_mpi_SOURCES = hello_mpi.c
#femto_mpi_cp_SOURCES = mpi_cp.cc
#mpi_utils_test_SOURCES = mpi_utils_test.cc
#
#TESTS = $(check_PROGRAMS) $(check_SCRIPTS)
#
