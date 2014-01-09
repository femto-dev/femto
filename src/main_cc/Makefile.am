AM_CPPFLAGS = -I$(srcdir)/../utils -I$(srcdir)/../utils_cc -I$(srcdir)/../main

#LDADD = ../main/libfemto_seq.la ../main/libfemto.la ../main/libfemto_bwt.la ../utils_cc/libfemto_util_cc.la ../utils/libfemto_util.la -lssl -lcrypto
LDADD = ../main/libfemto_seq.la ../main/libfemto.la ../utils/libfemto_util.la

bin_PROGRAMS = femto_search 

femto_search_SOURCES = search_tool.cc
femto_search_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
femto_search_CPPFLAGS = -I $(srcdir)/../re2/re2 $(AM_CPPFLAGS)

TESTS = $(check_PROGRAMS) $(check_SCRIPTS)

