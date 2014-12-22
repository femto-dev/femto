set(FEMTO_UTIL_SRCS
    bit_array.h
    bit_funcs.h
    bswap.h
    buffer.h
    buffer_funcs.h
    buffer.c
    cache.c
    cache.h
    error.c
    error.h
    file_find.c
    file_find.h
    hashmap.c
    hashmap.h
    page_utils.h
    page_utils.c
    queue_map.c
    queue_map.h
    sysqueue.h
    systree.h
    timing.c
    timing.h
    util.c
    util.h
    processors.h
    processors.c
    iostats.h
    iostats.c
    string_sort.h
    string_sort.c
    suffix_sort.h
    suffix_sort.c)

set(UTIL_TESTS
    bit_funcs_test
    bit_array_test
    buffer_test
    cache_test
    hashmap_test
    mmap_test
    page_utils_test
    queue_map_test
    util_test)

add_library(femto_util STATIC ${FEMTO_UTIL_SRCS})


foreach(test ${UTIL_TESTS})
  add_executable(${test} EXCLUDE_FROM_ALL ${test}.c)
  add_test(${test} ${test})
  target_link_libraries(${test} femto_util ${DEFAULT_LIBRARIES})
  add_dependencies(check ${test})
endforeach(test)



#original Makefile.am contents follow:

#
#noinst_LTLIBRARIES = libfemto_util.la
#
#libfemto_util_la_SOURCES = bit_array.h bit_funcs.h bswap.h buffer.h buffer_funcs.h buffer.c cache.c cache.h error.c error.h file_find.c file_find.h  hashmap.c hashmap.h page_utils.h page_utils.c queue_map.c queue_map.h sysqueue.h systree.h timing.c timing.h util.c util.h processors.h processors.c iostats.h iostats.c
#
#LDADD = libfemto_util.la
#
#TESTS = bit_funcs_test bit_array_test buffer_test cache_test hashmap_test mmap_test queue_map_test util_test page_utils_test
#
#check_PROGRAMS = bit_funcs_test bit_array_test buffer_test cache_test hashmap_test mmap_test queue_map_test util_test page_utils_test
#
## lost memtest
## linux only random_read_test
#
##bin_PROGRAMS = random_read_test find_util stats_util dorband_sort_test is_netfs
#
