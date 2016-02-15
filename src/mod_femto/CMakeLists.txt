# include ../util
include_directories(${CMAKE_SOURCE_DIR}/src/utils)
# include ../util_cc
#include_directories(${CMAKE_SOURCE_DIR}/src/utils_cc)
# include ../main
include_directories(${CMAKE_SOURCE_DIR}/src/main)
# include ../mpi
#include_directories(${CMAKE_SOURCE_DIR}/src/mpi)
# include ../re2
#include_directories(${CMAKE_SOURCE_DIR}/src/re2/re2/)

find_package(APR)
find_package(APXS)

if(APR_FOUND)
else(APR_FOUND)
  message(WARNING "APR not found, skipping mod_femto build")
endif(APR_FOUND)
if(APXS_FOUND)
else(APXS_FOUND)
  message(WARNING "APXS not found, skipping mod_femto build")
endif(APXS_FOUND)
if(MOD_FEMTO)
else(MOD_FEMTO)
  message(WARNING "set -DMOD_FEMTO=true to build mod_femto")
endif(MOD_FEMTO)

if(MOD_FEMTO)
  if(APR_FOUND)
    if(APXS_FOUND)
      #apxs-install:
      #	${APXS} -i -a -n femto libmod_femto.la
      #
      #    execute_process(COMMAND ${APXS} -q INCLUDEDIR OUTPUT_VARIABLE APXS_INCLUDE)
      #include_directories(${APXS_INCLUDE})

      add_library(libmod_femto mod_femto.c)
      target_link_libraries(libmod_femto ${APR_LIBRARY} ${APU_LIBRARY})
      include_directories(${APXS_INCLUDE_DIR} ${APR_INCLUDE_DIR} ${APU_INCLUDE_DIR})
      if(APR_CPPFLAGS)
        add_definitions("${APR_CPPFLAGS}")
      endif(APR_CPPFLAGS)

      add_definitions(-D_LARGEFILE64_SOURCE -D_GNU_SOURCE)
      #building just a library. 
      target_link_libraries(libmod_femto libfemto libfemto_util -module)
    endif(APXS_FOUND)
  endif(APR_FOUND)
endif(MOD_FEMTO)

#APXS_BIN is path to APXS
#add_custom_command(TARGET target
#                   PRE_BUILD | PRE_LINK | POST_BUILD
#                   COMMAND command1 [ARGS] [args1...]
#                   [COMMAND command2 [ARGS] [args2...] ...]
#                   [WORKING_DIRECTORY dir]
#                   [COMMENT comment] [VERBATIM])
#
#include_directories(${KDE4_INCLUDES} ${KDE4_INCLUDE_DIR} ${QT_INCLUDES} )


########### install files ###############




#original Makefile.am contents follow:

#
#if HAS_APXS
#
#AM_CPPFLAGS = -I/usr/include/apr-1 -I/usr/include/apr-1.0 -I$(srcdir)/../main -I`${APXS} -q INCLUDEDIR` -D_LARGEFILE64_SOURCE -D_GNU_SOURCE
#
#pkglib_LTLIBRARIES = libmod_femto.la
#libmod_femto_la_SOURCES = mod_femto.c
#libmod_femto_la_LIBADD = ../main/libfemto.la ../utils/libfemto_util.la
#libmod_femto_la_LDFLAGS = -module
#
#EXTRA_DIST = README femto-js
#
#apxs-install:
#	${APXS} -i -a -n femto libmod_femto.la
#
#reload:
#	apachectl stop
#	${APXS} -i -a -n femto libmod_femto.la
#	apachectl start
#
#install-js:
#	rm -Rf /usr/local/apache2/htdocs/femto-js.old
#	mv /usr/local/apache2/htdocs/femto-js /usr/local/apache2/htdocs/femto-js.old
#	rsync -av femto-js/ /usr/local/apache2/htdocs/femto-js/
#
#endif
#
