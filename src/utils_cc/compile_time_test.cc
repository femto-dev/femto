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

  femto/src/utils_cc/compile_time_test.cc
*/
#include <iostream>
#include <cassert>

int fun(void)
{
  assert(0);
}
#include "compile_time.hh"

int main(int argc, char** argv)
{
  // Check pow
  assert((CompileTimePow<1,1>::ret == 1));
  assert((CompileTimePow<1,2>::ret == 1));
  assert((CompileTimePow<1,3>::ret == 1));
  assert((CompileTimePow<2,1>::ret == 2));
  assert((CompileTimePow<2,2>::ret == 4));
  assert((CompileTimePow<2,3>::ret == 8));
  assert((CompileTimePow<2,4>::ret == 16));
  assert((CompileTimePow<256,1>::ret == 256));
  assert((CompileTimePow<257,1>::ret == 257));
  assert((CompileTimePow<256,2>::ret == 65536));
  assert((CompileTimePow<257,2>::ret == 66049));
  assert((CompileTimePow<256,3>::ret == 16777216));
  assert((CompileTimePow<257,3>::ret == 16974593));

  // Check CompileTimeNumBytes
  assert(CompileTimeNumBytes<0x0ULL>::num_bytes == 1);
  assert(CompileTimeNumBytes<0xffULL>::num_bytes == 1);
  assert(CompileTimeNumBytes<0x100ULL>::num_bytes == 2);
  assert(CompileTimeNumBytes<0xffffULL>::num_bytes == 2);
  assert(CompileTimeNumBytes<0x10000ULL>::num_bytes == 3);
  assert(CompileTimeNumBytes<0xffffffULL>::num_bytes == 3);
  assert(CompileTimeNumBytes<0x1000000ULL>::num_bytes == 4);
  assert(CompileTimeNumBytes<0xffffffffULL>::num_bytes == 4);
  assert(CompileTimeNumBytes<0x100000000ULL>::num_bytes == 5);
  assert(CompileTimeNumBytes<0xffffffffffULL>::num_bytes == 5);
  assert(CompileTimeNumBytes<0x10000000000ULL>::num_bytes == 6);
  assert(CompileTimeNumBytes<0xffffffffffffULL>::num_bytes == 6);
  assert(CompileTimeNumBytes<0x1000000000000ULL>::num_bytes == 7);
  assert(CompileTimeNumBytes<0xffffffffffffffULL>::num_bytes == 7);
  assert(CompileTimeNumBytes<0x100000000000000ULL>::num_bytes == 8);
  assert(CompileTimeNumBytes<0xffffffffffffffffULL>::num_bytes == 8);

  std::cout << "All tests PASS" << std::endl;
  return 0;
}
