/*
  (*) 2014-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/density.h
*/

#include <stdio.h> 
#include <stdlib.h> 
#include <inttypes.h> 
#include <assert.h> 

#include "bit_funcs.h"

uint64_t density_choose(int n, int k);
uint64_t density_offset_to_number(int n, int d /*density*/, uint64_t index);
uint64_t density_number_to_offset(int n, int d, uint64_t num);

struct density_offset {
  uint64_t density;
  uint64_t offset;
};

struct density_offset density_number_to_density_offset(int n, uint64_t num);
