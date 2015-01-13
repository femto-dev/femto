/*
  (*) 2007-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/dcx_inmem.hh
*/


#ifndef _DCX_INMEM_HH_
#define _DCX_INMEM_HH_

extern "C" {
  #include "error.h"
  #include "suffix_sort.h"
}

static const int DCX_FLAG_SOMETIMES_NAME = 1;
static const int DCX_FLAG_USE_TWO_STAGE = 2;
static const int DCX_FLAG_USE_TWO_STAGE_SINGLE = 4;
static const int DCX_FLAG_USE_TWO_STAGE_DOUBLE = 8;

error_t dcx_inmem_ssort(suffix_sorting_problem_t* p, int period, int flags=DCX_FLAG_USE_TWO_STAGE);

int dcx_inmem_supports_period(int period);

// returns the number of padding characters (you must
// multiply by bytes per characters) required for guaranteed
// sucessfull completion.
sptr_t dcx_inmem_get_padding_chars(int period);

#include "two_stage.hh"

#endif

