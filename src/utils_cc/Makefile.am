AM_CPPFLAGS = -I$(srcdir)/../utils

noinst_LTLIBRARIES = libfemto_util_cc.la

libfemto_util_cc_la_SOURCES = error.hh error_exception.cc utils.hh mmap.hh mmap.cc utils.cc varint.hh varint.cc pipelining.hh pipelining.cc record.hh criterion.hh compare_record.hh compare_record.cc sort_context.hh merger.hh merge_node.hh merge_node.cc print_records_node.hh file_pipe.hh file_pipe.cc file_pipe_context.hh file_pipe_context.cc compile_time.hh compile_time.cc distribute_utils.hh distribute_utils.cc bucket_sort.hh bucket_sort.cc file_find_node.hh file_find_node.cc distributor_node.hh distributor_node.cc sort_basic.hh sort_basic.cc sort.hh sort.cc io_pipe.hh io_pipe.cc string_sort.cc
# removed: bucket_trie.hh bucket_trie.cc form_runs_node.hh form_runs_node.cc sort_node.hh sort_node.cc
if HAS_LINUX_LIBAIO
libfemto_util_cc_la_SOURCES += kaio_pipe.hh kaio_pipe.cc
endif
if HAS_POSIX_AIO
libfemto_util_cc_la_SOURCES += aio_pipe.hh aio_pipe.cc
endif

LDADD = libfemto_util_cc.la ../utils/libfemto_util.la

check_PROGRAMS = compare_record_test pipelining_test varint_test merger_test compile_time_test bucket_sort_test sort_unit_test string_sort_test
# removed: bucket_trie_test form_runs_node_test sort_node_test
bin_PROGRAMS = femto_sort_tests

pipelining_test_SOURCES = pipelining_test.cc
compare_record_test_SOURCES = example_record.hh compare_record_test.cc
varint_test_SOURCES = varint_test.cc
merger_test_SOURCES = merger_test.cc
#sort_node_test_SOURCES = sort_node_test.cc
#form_runs_node_test_SOURCES= example_record.hh form_runs_node_test.cc
compile_time_test_SOURCES = compile_time_test.cc
femto_sort_tests_SOURCES = sort_tests.hh sort_tests.cc
#bucket_trie_test_SOURCES = example_record.hh bucket_trie_test.cc
bucket_sort_test_SOURCES = sort_tests.hh bucket_sort_test.cc
sort_unit_test_SOURCES = sort_tests.hh sort_unit_test.cc
string_sort_test_SOURCES = string_sort_test.cc

TESTS = $(check_PROGRAMS) $(check_SCRIPTS)

EXTRA_DIST = CMakeLists.txt
