/*
  (*) 2010-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/dcover_test.cc
*/
#include <cstdio>

#include "dcover.hh"

int main(int argc, char** argv)
{


  // Figure out which samples to use for DC3 and DC7;
  // these will be checked manually.

  {
    Dcover<3> cover;
    int table[3][3] =
    {
      {0, 0, 1},
      {0, 0, 1},
      {0, 1, 0}
    };

    for( int i = 0; i < 3; i++ ) {
      for( int j = 0; j < 3; j++ ) {
        assert(table[i][j] == cover.get_table(i,j));
      }
    }
  }

  {
    Dcover<7> cover;
    int table[7][7] =
    {
    //  0  1  2  3  4  5  6
 /*0*/ {0, 0, 1, 0, 2, 2, 1},
 /*1*/ {0, 0, 2, 0, 2, 1, 1},
 /*2*/ {0, 2, 0, 1, 2, 1, 0},
 /*3*/ {0, 0, 2, 0, 1, 2, 1},
 /*4*/ {0, 2, 2, 1, 0, 0, 1},
 /*5*/ {1, 0, 2, 2, 1, 0, 0},
 /*6*/ {0, 1, 0, 2, 2, 1, 0},
    };

    for( int i = 0; i < 7; i++ ) {
      for( int j = 0; j < 7; j++ ) {
        assert(table[i][j] == cover.get_table(i,j));
      }
    }
  }


  printf("All tests pass\n");
  return 0;
}

