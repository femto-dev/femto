/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

    * This is a work of the United States Government and is not protected by
      copyright in the United States.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  femto/src/utils/bswap.h
*/
#ifndef _BSWAP_H_
#define _BSWAP_H_

// We use __LINUX__ but some gcc
// has only __linux__ ... here we
// make it work.
#ifdef __linux__
#ifndef __LINUX__
#define __LINUX__ 1
#endif
#endif

#ifdef _BIG_ENDIAN

/* host byte order is network byte order. */
# define ntoh_16(x) (x)
# define hton_16(x) (x)

# define ntoh_32(x) (x)
# define hton_32(x) (x)

# define ntoh_64(x) (x)
# define hton_64(x) (x)

#else

# ifdef __APPLE__
# include <libkern/OSByteOrder.h>
# endif

# ifdef OSSwapInt64
/* Return a value with all bytes in the 16 bit argument swapped.  */
#  define ntoh_16(x) (OSSwapInt16(x))
#  define hton_16(x) (OSSwapInt16(x))

/* Return a value with all bytes in the 32 bit argument swapped.  */
#  define ntoh_32(x) (OSSwapInt32(x))
#  define hton_32(x) (OSSwapInt32(x))

/* Return a value with all bytes in the 64 bit argument swapped.  */
#  define ntoh_64(x) (OSSwapInt64(x))
#  define hton_64(x) (OSSwapInt64(x))

# else

#  ifdef __LINUX__
// hopefully on a little-endian machine the byte-swapping
// asm exists. (true for linux; maybe need to copy from there
// to support non-linux on little endian machines).
#   include <byteswap.h>

/* Return a value with all bytes in the 16 bit argument swapped.  */
#   define ntoh_16(x) (bswap_16(x))
#   define hton_16(x) (bswap_16(x))

/* Return a value with all bytes in the 32 bit argument swapped.  */
#   define ntoh_32(x) (bswap_32(x))
#   define hton_32(x) (bswap_32(x))

/* Return a value with all bytes in the 64 bit argument swapped.  */
#   define ntoh_64(x) (bswap_64(x))
#   define hton_64(x) (bswap_64(x))

#  else

// not linux
// note -- solaris has BSWAP_64, byteorder.h, ntohll
#   include <arpa/inet.h>

#   define ntoh_16(x) (ntohs(x))
#   define hton_16(x) (htons(x))
#   define ntoh_32(x) (ntohl(x))
#   define hton_32(x) (htonl(x))

#   if defined __x86_64__
     // 64-bit
     // copied from linux /usr/include/byteswap.h
#    define my_bswap_64(x) \
     (__extension__							      \
      ({ register unsigned long __v, __x = (x);				      \
	   __asm__ ("bswap %q0" : "=r" (__v) : "0" (__x));		      \
	 __v; }))
#   else
     // not 64-bit
     // copied from linux /usr/include/byteswap.h
#  define my_bswap_64(x) \
     (__extension__                                                           \
      ({ union { __extension__ unsigned long long int __ll;                   \
                 unsigned int __l[2]; } __w, __r;                             \
           {                                                                  \
             __w.__ll = (x);                                                  \
             __r.__l[0] = ntoh_32 (__w.__l[1]);                            \
             __r.__l[1] = ntoh_32 (__w.__l[0]);                            \
           }                                                                  \
         __r.__ll; }))

#   endif

#   define ntoh_64(x) (my_bswap_64(x))
#   define hton_64(x) (my_bswap_64(x))

// end 
#  endif

// end if OSSwapInt64
# endif 


// end #ifdef _BIG_ENDIAN
#endif

// end #ifndef _BSWAP_H
#endif
