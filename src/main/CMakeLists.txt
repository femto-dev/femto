SET(BISON_INPUT ${CMAKE_SOURCE_DIR}/src/main/posix.bison.y)
SET(BISON_OUTPUT ${CMAKE_SOURCE_DIR}/src/main/posix.bison.c)
SET(BISON_DEFINES ${CMAKE_SOURCE_DIR}/src/main/posix.bison.h)
SET(FLEX_INPUT ${CMAKE_SOURCE_DIR}/src/main/posix.flex.l)
SET(FLEX_OUTPUT ${CMAKE_SOURCE_DIR}/src/main/posix.flex.c)
set(FEMTO_REGEXP_SRCS
    ${BISON_OUTPUT}
    ${FLEX_OUTPUT}
    ast.h
    ast.c
    compile_regexp.h
    compile_regexp.c
    nfa.h
    nfa.c
    query_planning.h
    query_planning.c)
SET(FEMTO_SEQ_SRCS
    sequential_search.h
    sequential_search.c)
SET(FEMTO_INDEX_SRCS
    block_storage.h
    block_storage.c
    bzlib_private.h
    huffman.c
    server.h
    server.c
    wtree.h
    wtree_funcs.h
    wtree.c
    index.h
    index.c
    results.h
    results.c
    index_merge.h
    index_merge.c
    femto_internal.h
    json.h
    json.c
    femto.c)
SET(FEMTO_BWT_SRCS
    index_types.h
    bwt_writer.h
    bwt_writer.c
    bwt.h
    bwt_reader.h
    bwt_reader.c
    bwt_prepare.h
    bwt_prepare.c
    construct.h
    construct.c)

if(NO_FLEX_BISON)
  # don't try to run flex or bison
else(NO_FLEX_BISON)

  FIND_PACKAGE(BISON REQUIRED)
  IF(BISON_FOUND)
      ADD_CUSTOM_COMMAND(
        OUTPUT ${BISON_OUTPUT} ${BISON_DEFINES}
        COMMAND ${BISON_EXECUTABLE}
                --output=${BISON_OUTPUT}
                --defines=${BISON_DEFINES}
                ${BISON_INPUT}
        MAIN_DEPENDENCY ${BISON_INPUT}
        COMMENT "Generating posix.bison.c and posix.bison.h"
      )
  ENDIF(BISON_FOUND)

  FIND_PACKAGE(FLEX REQUIRED)
  IF(FLEX_FOUND)
      ADD_CUSTOM_COMMAND(
        OUTPUT ${FLEX_OUTPUT}
        COMMAND ${FLEX_EXECUTABLE}
                --outfile=${FLEX_OUTPUT}
                ${FLEX_INPUT}
        MAIN_DEPENDENCY ${FLEX_INPUT}
        COMMENT "Generating posix.flex.c"
      )
  ENDIF(FLEX_FOUND)

endif(NO_FLEX_BISON)

# include ../util
include_directories(${CMAKE_SOURCE_DIR}/src/utils)


add_library(femto STATIC ${FEMTO_REGEXP_SRCS} ${FEMTO_INDEX_SRCS})
add_library(femto_bwt STATIC ${FEMTO_BWT_SRCS})
add_library(femto_seq STATIC ${FEMTO_SEQ_SRCS})

add_executable(femto_multiquery query_tool.c)
target_link_libraries(femto_multiquery femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
add_executable(femto_flatten flatten_tool.c)
target_link_libraries(femto_flatten femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
add_executable(femto_handle_request handle_request.c)
target_link_libraries(femto_handle_request femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
add_executable(femto_info info_tool.c)
target_link_libraries(femto_info femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
add_executable(femto_common common_tool.c)
target_link_libraries(femto_common femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
install(TARGETS femto_flatten femto_handle_request femto_info femto_common
        ${INSTALL_TARGETS_DEFAULT_ARGS})

set(FEMTO_TESTS
    nfa_test
    query_planning_test
    results_test
    bwt_test
    bseq_test
    wtree_test)

foreach(test ${FEMTO_TESTS})
  add_executable(${test} EXCLUDE_FROM_ALL ${test}.c)
  add_test(${test} ${test})
  target_link_libraries(${test} femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
  add_dependencies(check ${test})
endforeach(test)

set(BWT_TESTS
    index_test
    query_test
    regexp_test)

foreach(test ${BWT_TESTS})
  add_executable(${test} EXCLUDE_FROM_ALL ${test}.c bwt_creator.h bwt_creator.c bwt_qsufsort.c)
  add_test(${test} ${test})
  target_link_libraries(${test} femto femto_bwt femto_util ${DEFAULT_LIBRARIES})
  add_dependencies(check ${test})
endforeach(test)

install(FILES  femto.h DESTINATION include)



#original Makefile.am contents follow:

#AM_CPPFLAGS = -I$(srcdir)/../utils
#
#noinst_LTLIBRARIES = libfemto.la libfemto_bwt.la libfemto_seq.la
#
#BUILT_SOURCES = posix.bison.h posix.bison.c posix.flex.c
#AM_YFLAGS = -d
#
#regexp_sources = posix.bison.y posix.flex.l ast.h ast.c compile_regexp.h compile_regexp.c nfa.h nfa.c query_planning.h query_planning.c
#seq_sources = sequential_search.h sequential_search.c
#index_sources = block_storage.h block_storage.c bzlib_private.h huffman.c server.h server.c wtree.h wtree_funcs.h wtree.c index.h index.c results.h results.c index_merge.h index_merge.c femto_internal.h json.h json.c femto.c
#bwt_sources = index_types.h bwt_writer.h bwt_writer.c bwt.h bwt_reader.h bwt_reader.c bwt_prepare.h bwt_prepare.c construct.h construct.c
#
#libfemto_la_SOURCES = $(regexp_sources) $(index_sources)
#include_HEADERS = femto.h
#libfemto_bwt_la_SOURCES = $(bwt_sources)
#libfemto_seq_la_SOURCES = $(seq_sources)
#
#LDADD = libfemto.la libfemto_bwt.la ../utils/libfemto_util.la
#
#TESTS = nfa_test query_planning_test results_test index_test bwt_test bseq_test wtree_test query_test regexp_test
## turned off merge_test
#noinst_DATA = test_indexes
#test_indexes:
#	mkdir test_indexes
#clean-local:
#	rm -Rf test_indexes
#
#check_PROGRAMS = nfa_test query_planning_test results_test index_test bwt_test bseq_test wtree_test query_test regexp_test 
#
#index_test_SOURCES = index_test.c bwt_creator.h bwt_creator.c bwt_qsufsort.c
##merge_test_SOURCES = merge_test.c bwt_creator.h bwt_creator.c bwt_qsufsort.c
#query_test_SOURCES = query_test.c bwt_creator.h bwt_creator.c bwt_qsufsort.c
#regexp_test_SOURCES = regexp_test.c bwt_creator.h bwt_creator.c bwt_qsufsort.c
#
#bin_PROGRAMS = femto_flatten femto_handle_request femto_info femto_flatten femto_common
#noinst_PROGRAMS = femto_multiquery
#
#femto_multiquery_SOURCES = query_tool.c
#femto_flatten_SOURCES = flatten_tool.c
#femto_handle_request_SOURCES = handle_request.c
#femto_info_SOURCES = info_tool.c
#femto_common_SOURCES = common_tool.c
#
#EXTRA_DIST = block_format.txt update_format.txt bwt_format.txt compile_regexp_test.c flex_bison_parser.h flex_bison_parser.c index_test_funcs.c nfa_approx_tool.h nfa_approx_tool.c nfa_tool.c parse_regexp.c qsufsort.c QUERY_FORMAT.txt
#
