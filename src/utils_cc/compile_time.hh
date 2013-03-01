/*
  (*) 2008-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/compile_time.hh
*/
#ifndef _COMPILE_TIME_HH_
#define _COMPILE_TIME_HH_

/* Some template-meta-programming classes for
   doing compile-time computations. */

// Returns pow(x,y) or x^y or x**y.
// y must be positive..
template < int x,
           int y >
struct CompileTimePow
{
  enum { ret = (x*(CompileTimePow<x,y-1>::ret)) };
};
template < int x >
struct CompileTimePow<x,0>
{
  enum { ret = 1 };
};



/* Assuming that we've got to store 0..max inclusive,
   how many bytes of big-endian numbers should we use?
   */
template <unsigned long long max>
struct CompileTimeNumBytes {
  enum { num_bytes =
         (max<(1ULL<<8))?(1):
           ((max<(1ULL<<16))?(2):
             ((max<(1ULL<<24))?(3):
               ((max<(1ULL<<32))?(4):
                 ((max<(1ULL<<40))?(5):
                   ((max<(1ULL<<48))?(6):
                     ((max<(1ULL<<56))?(7):
                       (8)
                     )
                   )
                 )
               )
             )
           ) };
};


#endif
