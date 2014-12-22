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

  femto/src/dcx_cc/dcover.cc
*/
#include "dcover.hh"

template<int Period> const Dcover<Period> Dcover<Period>::g;

#define CHECK_COVER(p) \
  Dcover<p>::g.in_cover(Dcover<p>::g.get_cover(0))

/*
MAKE_COVER(3);
MAKE_COVER(7);
MAKE_COVER(13);
MAKE_COVER(21);
MAKE_COVER(31);
MAKE_COVER(39);
MAKE_COVER(57);
MAKE_COVER(73);
MAKE_COVER(91);
MAKE_COVER(95);
MAKE_COVER(133);
MAKE_COVER(1024);
MAKE_COVER(4096);
MAKE_COVER(8192);
*/

// Make sure that the right ones exist!
// This is really here as a dodge for linker errors.
int dcover_check(void)
{
  CHECK_COVER(3);
  CHECK_COVER(7);
  CHECK_COVER(13);
  CHECK_COVER(21);
  CHECK_COVER(31);
  CHECK_COVER(39);
  CHECK_COVER(57);
  CHECK_COVER(73);
  CHECK_COVER(91);
  CHECK_COVER(95);
  CHECK_COVER(133);
//  CHECK_COVER(1024);
//  CHECK_COVER(4096);
//  CHECK_COVER(8192);

  return 0;
}

