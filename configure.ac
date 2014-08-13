#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(femto, 1.2.9.1, michaelferguson@acm.org)
AM_INIT_AUTOMAKE([1.9 foreign tar-pax])
AC_CONFIG_SRCDIR([src/main/bwt_writer.h])
AM_CONFIG_HEADER([config.h])

# did the user specify custom C flags?
CUSTOMCFLAGS=$CFLAGS
CUSTOMCXXFLAGS=$CXXFLAGS
# add a single space to trick AC_PROG_CC into
# leaving no CFLAGS (default is to add -g -O2)
CFLAGS="$CFLAGS $EXTRA_CFLAGS"
CXXFLAGS="$CXXFLAGS $EXTRA_CFLAGS"

AC_ARG_ENABLE([debug], AS_HELP_STRING([--enable-debug], [turn on debug]),
[
DEBUG=yes
],
[
DEBUG=""
])

AC_ARG_ENABLE([profile], AS_HELP_STRING([--enable-profile], [turn on gprof profiling (-pg)]),
[
PROFILE=yes
],
[
PROFILE=""
])


AC_ARG_WITH([mpi], AS_HELP_STRING([--with-mpi], [build with MPI ie mpicc and mpic++]),
[],
[with_mpi=check])

# Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AM_PROG_CC_C_O

AS_IF([test "x$with_mpi" != xno],
      [
        # Check for MPI compiler wrappers.
        # Enable them if possible.
        AC_CHECK_PROG([MPICC],[mpicc],[mpicc],[])
        AC_CHECK_PROG([MPICXX],[mpic++],[mpic++],[])
        AS_IF([test "x$with_mpi" != xcheck],
              [
               AS_IF([test "x" == "x$MPICC"],
                [AC_MSG_FAILURE([--with-mpi was given, but no mpicc found])])
               ])
        # Switch to these compilers.
        #CC=$MPICC
        #CXX=$MPICXX
      ])

AS_IF([test "x$MPICC" == "x"],
       [
         MPICC=$CC
         MPICXX=$CXX
       ],
       [
         AC_DEFINE([HAVE_MPI_H],[1],[Has MPI])
       ])

AC_PROG_LIBTOOL
AC_PROG_YACC
AM_PROG_LEX

# Check for Apache eXtenSion Tool apxs
AC_CHECK_PROG([APXS],[apxs],[apxs])
AC_CHECK_PROG([APXS],[apxs2],[apxs2])

AS_IF([test "x$APXS" == "x"],
      [AM_CONDITIONAL([HAS_APXS], [false])],
      [AM_CONDITIONAL([HAS_APXS], [true])])

# Checks for libraries.
AC_CHECK_LIB([rt], [clock_gettime])
AC_CHECK_LIB([c],[aio_read],
             [POSIX_AIO_LIB=-lc
              AC_DEFINE([HAS_POSIX_AIO],[1],[Has POSIX AIO e.g. aio_read])
              AM_CONDITIONAL([HAS_POSIX_AIO], [true])],
             [AC_CHECK_LIB([rt], [aio_read],
                           [POSIX_AIO_LIB=-lrt
                            AC_DEFINE([HAS_POSIX_AIO],[1],[Has POSIX AIO e.g. aio_read])
                            AM_CONDITIONAL([HAS_POSIX_AIO], [true])],
                           [POSIX_AIO_LIB=
                            AM_CONDITIONAL([HAS_POSIX_AIO], [false])])])
AC_CHECK_LIB([aio], [io_submit],
             [AM_CONDITIONAL([HAS_LINUX_LIBAIO], [true])
              AC_DEFINE([HAS_LINUX_LIBAIO],[1],[Has linux AIO e.g. io_submit])]
              AC_CHECK_LIB([aio], [io_submit]),
             [AM_CONDITIONAL([HAS_LINUX_LIBAIO], [false])])

AC_CHECK_LIB([ssl], [SSL_library_init])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([inttypes.h limits.h stddef.h stdlib.h string.h strings.h unistd.h malloc.h fcntl.h fcntl.h sys/statvfs.h])

AC_LANG_PUSH(C++)
AC_CHECK_HEADERS([parallel/algorithm])
AC_LANG_POP(C++)

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_C_RESTRICT
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_STAT
AC_CHECK_FUNCS([clock_gettime floor ftruncate memset mkdir munmap strdup strerror sync_file_range posix_fadvise fstatfs readahead])
AC_CHECK_DECLS([__sync_fetch_and_add])

# Custom check to append a default CFLAG if it is available.
# ------
AC_DEFUN([AX_ADD_CFLAG],
[
AC_LANG_PUSH(C)
AC_REQUIRE([AC_LANG_PROGRAM])
AC_MSG_CHECKING([C compiler flags for $1])
SAVECFLAGS=$CFLAGS
CFLAGS="$CFLAGS $1"
AC_RUN_IFELSE(
              [AC_LANG_PROGRAM([
                               ],
                               [
                                 return 0;
                               ])],
              [
                AC_MSG_RESULT(yes)
              ],
              [
                AC_MSG_RESULT(no)
                CFLAGS=$SAVECFLAGS
              ] )
unset SAVECFLAGS
AC_LANG_POP(C)
])# AX_ADD_CFLAG

# Custom check to append a default CFLAG if it is available.
# ------
AC_DEFUN([AX_ADD_CXXFLAG],
[
AC_LANG_PUSH(C++)
AC_REQUIRE([AC_LANG_PROGRAM])
AC_MSG_CHECKING([C++ compiler flags for $1])
SAVECXXFLAGS=$CXXFLAGS
CXXFLAGS="$CXXFLAGS $1"
AC_RUN_IFELSE(
              [AC_LANG_PROGRAM([
                               ],
                               [
                                 return 0;
                               ])],
              [
                AC_MSG_RESULT(yes)
              ],
              [
                AC_MSG_RESULT(no)
                CXXFLAGS=$SAVECXXFLAGS
              ] )
unset SAVECXXFLAGS
AC_LANG_POP(C++)
])# AX_ADD_CFLAG

# Add our default flags if no custom flags were specified.
[if [ -z "$CUSTOMCFLAGS" ]]
then
AC_MSG_NOTICE([Using default C compiler flags])
  AX_ADD_CFLAG([-std=gnu99])
  AX_ADD_CFLAG([-Wall])
  AX_ADD_CFLAG([-m64 -D_64_BIT])
  # Add support for OpenMP if available
  AX_ADD_CFLAG([-fopenmp])
  AX_ADD_CFLAG([-fnested-functions])
  # This flag is needed on older redhats to get posix read/write locks
  AX_ADD_CFLAG([-D_XOPEN_SOURCE=600])

  [if [ "$PROFILE" ]]
  then
    AX_ADD_CFLAG([-pg])
  fi

  [if [ -z "$DEBUG" ]]
  then
    AX_ADD_CXXFLAG([-g])
    AX_ADD_CFLAG([-O4])
    AX_ADD_CFLAG([-fomit-frame-pointer])
    AX_ADD_CFLAG([-DEXTRA_CHECKS=0])
    AX_ADD_CFLAG([-march=native])
    AX_ADD_CFLAG([-mtune=native])
  else
    AX_ADD_CFLAG([-g])
    AX_ADD_CFLAG([-DEXTRA_CHECKS=1])
  fi
else
AC_MSG_NOTICE([Using provided C compiler flags])
fi

[if [ -z "$CUSTOMCXXFLAGS" ]]
then
AC_MSG_NOTICE([Using default C++ compiler flags])
#  AX_ADD_CXXFLAG([-std=gnu99])
  AX_ADD_CXXFLAG([-Wall])
#  AX_ADD_CXXFLAG([-Weffc++]) These seem to be noisy and not totally useful.
  AX_ADD_CXXFLAG([-m64 -D_64_BIT])
#  AX_ADD_CXXFLAG([-fnested-functions]) not supported in C++
  # Add support for OpenMP if available
  AX_ADD_CXXFLAG([-fopenmp])
  # This flag is needed on older redhats to get posix read/write locks
  AX_ADD_CXXFLAG([-D_XOPEN_SOURCE=600])

  [if [ "$PROFILE" ]]
  then
    AX_ADD_CXXFLAG([-pg])
  fi

  [if [ -z "$DEBUG" ]]
  then
    AX_ADD_CXXFLAG([-g])
    AX_ADD_CXXFLAG([-O4])
    AX_ADD_CXXFLAG([-fomit-frame-pointer])
    AX_ADD_CXXFLAG([-DEXTRA_CHECKS=0])
    AX_ADD_CXXFLAG([-march=native])
    AX_ADD_CXXFLAG([-mtune=native])
  else
    AX_ADD_CXXFLAG([-g])
    AX_ADD_CXXFLAG([-DEXTRA_CHECKS=1])
  fi
else
AC_MSG_NOTICE([Using provided C++ compiler flags])
fi


AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/re2/Makefile
                 src/utils/Makefile
                 src/utils_cc/Makefile
                 src/mpi/Makefile
                 src/main/Makefile
                 src/main_cc/Makefile
		 src/dcx_cc/Makefile
		 src/test/Makefile
		 src/scripts/Makefile
		 src/mod_femto/Makefile
		 ])
AC_OUTPUT
