/*
  (*) 2010-2014 Michael Ferguson <michaelferguson@acm.org>

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


template<int Period>
static void check_cover(){
  typedef Dcover<Period> cover_t;

  int max = 2 * cover_t::period + 10;
  // go out to 2*period.
  for( int i = 0; i < max; i++ ) {
    for( int j = 0; j < max; j++ ) {
      int i_off, j_off;
      int i_soff, j_soff;
      i_off = j_off = 0;
      i_soff = j_soff = 0;
      // check that we can compute l
      cover_t::g.which_offsets_to_use(i%cover_t::period, j%cover_t::period, &i_off, &j_off);
      // check that i+l and j+l are both
      // in the difference cover.
      assert(cover_t::g.in_cover((i+i_off) % cover_t::period));
      assert(cover_t::g.in_cover((j+j_off) % cover_t::period));

      if( cover_t::table_period == cover_t::period ) {
        cover_t::g.which_samples_to_use(i%cover_t::period, j%cover_t::period, &i_soff, &j_soff);
        // check that i+i_soff and j+j_soff are both in the
        // difference cover.
        int n_samples, ii, jj;
        // go forward until we get to a sample position.
        for( ii = i; !cover_t::g.in_cover(ii%cover_t::period); ii++) ;
        // now move forward i_soff sample positions.
        for( n_samples = 0; n_samples < i_soff; ) {
          if(cover_t::g.in_cover(ii%cover_t::period)) n_samples++;
          ii++;
        }
        // go forward until we get to a sample position
        for( ; !cover_t::g.in_cover(ii%cover_t::period); ii++) ;

        // go forward until we get to a sample position.
        for( jj = j; !cover_t::g.in_cover(jj%cover_t::period); jj++) ;
        for( n_samples = 0; n_samples < j_soff; ) {
          if(cover_t::g.in_cover(jj%cover_t::period)) n_samples++;
          jj++;
        }
        // go forward until we get to a sample position.
        for( ; !cover_t::g.in_cover(jj%cover_t::period); jj++) ;

        assert(cover_t::g.in_cover(ii%cover_t::period));
        assert(cover_t::g.in_cover(jj%cover_t::period));
        // and they should be a fixed distance apart (think l)
        assert(ii - i == jj - j);
      }
    }
  }
}

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


  check_cover<3>();
  check_cover<7>();
  check_cover<13>();
  check_cover<21>();
  check_cover<31>();
  check_cover<39>();
  check_cover<57>();
  check_cover<73>();
  check_cover<91>();
  check_cover<95>();
  check_cover<133>();
  //check_cover<1024>();
  //check_cover<2048>();
  //check_cover<4096>();
  //check_cover<8192>();



  printf("All tests pass\n");
  return 0;
}

