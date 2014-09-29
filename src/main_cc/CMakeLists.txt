# include ../util
include_directories(${CMAKE_SOURCE_DIR}/src/utils)
# include ../main
include_directories(${CMAKE_SOURCE_DIR}/src/main)
# include ../re2/re2
include_directories(${CMAKE_SOURCE_DIR}/src/re2/re2)


add_executable(femto_search search_tool.cc)
#link_directories(${RE2_LIB_DIR})
#target_link_libraries(femto_search re2 femto_seq femto femto_util)
target_link_libraries(femto_search ${RE2_LIB} femto_seq femto femto_util)
add_dependencies(femto_search make_re2)

install(TARGETS femto_search
        ${INSTALL_TARGETS_DEFAULT_ARGS})


########### install files ###############




#original Makefile.am contents follow:

#AM_CPPFLAGS = -I$(srcdir)/../utils -I$(srcdir)/../utils_cc -I$(srcdir)/../main
#
##LDADD = ../main/libfemto_seq.la ../main/libfemto.la ../main/libfemto_bwt.la ../utils_cc/libfemto_util_cc.la ../utils/libfemto_util.la -lssl -lcrypto
#LDADD = ../main/libfemto_seq.la ../main/libfemto.la ../utils/libfemto_util.la
#
#bin_PROGRAMS = femto_search 
#
#femto_search_SOURCES = search_tool.cc
#femto_search_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
#femto_search_CPPFLAGS = -I $(srcdir)/../re2/re2 $(AM_CPPFLAGS)
#
#TESTS = $(check_PROGRAMS) $(check_SCRIPTS)
#
