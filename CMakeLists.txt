cmake_minimum_required(VERSION 2.8)
project(FEMTO C CXX)

set(FEMTO_VERSION "femto 1.2.9.2 from http://github.com/femto-dev/femto")

# Find FindAPR and FindAPXS
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --std=gnu99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=gnu++11 -fdiagnostics-show-template-tree")

include(CheckLibraryExists)
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckFunctionExists)
include(CheckCSourceCompiles)

set(NO_FLEX_BISON 1)

if(NO_FLEX_BISON)
  # don't try to run flex or bison
else(NO_FLEX_BISON)
  find_package(FLEX)
  find_package(BISON)
endif(NO_FLEX_BISON)

find_package(Threads REQUIRED)
find_package(MPI)

#if(NOT ${BISON_VERSION} EQUAL:
CHECK_LIBRARY_EXISTS(rt clock_gettime "time.h" HAVE_CLOCK_GETTIME_RT)
CHECK_LIBRARY_EXISTS(c clock_gettime "time.h" HAVE_CLOCK_GETTIME_C)
CHECK_LIBRARY_EXISTS(rt aio_read "aio.h" HAS_POSIX_AIO_RT)
CHECK_LIBRARY_EXISTS(c aio_read "aio.h" HAS_POSIX_AIO_C)
CHECK_LIBRARY_EXISTS(aio io_submit "libaio.h" HAS_LINUX_LIBAIO)
CHECK_LIBRARY_EXISTS(ssl SSL_library_init "openssl/ssl.h" HAVE_LIBSSL)
CHECK_INCLUDE_FILE(sys/statvfs.h HAVE_STATVFS)
CHECK_INCLUDE_FILE_CXX(parallel/algorithm HAVE_PARALLEL_ALGORITHM)
#CHECK_FUNCTION_EXISTS(__sync_fetch_and_add HAVE_DECL___SYNC_FETCH_AND_ADD)
CHECK_C_SOURCE_COMPILES("int main(int argc, char** argv) { int* a = &argc; __sync_fetch_and_add(a, 1); }" HAVE_DECL___SYNC_FETCH_AND_ADD)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

# Probably not needed.
#CHECK_INCLUDE_FILE(inttypes.h HAVE_INTTYPES_H)
#CHECK_INCLUDE_FILE(limits.h HAVE_LIMITS_H)
#CHECK_INCLUDE_FILE(stddef.h HAVE_STDDEF_H)
#CHECK_INCLUDE_FILE(stdlib.h HAVE_STDLIB_H)
#CHECK_INCLUDE_FILE(string.h HAVE_STRING_H)
#CHECK_INCLUDE_FILE(strings.h HAVE_STRINGS_H)
#CHECK_INCLUDE_FILE(unistd.h HAVE_UNISTD_H)
#CHECK_INCLUDE_FILE(malloc.h HAVE_MALLOC_H)
#CHECK_INCLUDE_FILE(fcntl.h HAVE_FCNTL_H)

#Old things we checked for
### Checks for header files.
#AC_HEADER_DIRENT
#AC_HEADER_STDC
## Checks for library functions.
#AC_FUNC_CLOSEDIR_VOID
#AC_FUNC_MALLOC -- not used
#AC_FUNC_MEMCMP
#AC_FUNC_MMAP
#AC_FUNC_REALLOC
#AC_FUNC_STAT
#AC_CHECK_FUNCS([clock_gettime floor ftruncate memset mkdir munmap strdup strerro
  #  r sync_file_range posix_fadvise fstatfs readahead])
#AC_CHECK_DECLS([__sync_fetch_and_add])

enable_testing()

set(CMAKE_CXX_FLAGS "-g -Wall")

# Add libaries used everywhere
set(DEFAULT_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
if( DEFINED HAVE_CLOCK_GETTIME_RT OR DEFINED HAS_POSIX_AIO_RT)
  set(DEFAULT_LIBRARIES "${DEFAULT_LIBRARIES} -lrt")
endif()

# Emulate make check
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

#include config.h
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(INSTALL_TARGETS_DEFAULT_ARGS 
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)

add_subdirectory(src)

