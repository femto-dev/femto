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

  femto/src/utils_cc/criterion.hh
*/
#ifndef _SORTING_INTERFACE_HH_
#define _SORTING_INTERFACE_HH_

#include <cstddef>

#include "record.hh"

/* A sorting criterion is as follows.
 *
 * CHOICE 1: A comparison function:
 *
 * typedef compare_criterion_tag criterion_category;
 * int compare(a,b) const, returning 0 if equal, >0 if a>b, <0 if a<b
 *
 * CHOICE 2: Returns a key:
 *
 * typedef return_key_criterion_tag criterion_category;
 * typedef ... key_t;
 * key_t get_key(const Record& r) const
 *
 * Note the returned key can be a structure. If it is not a
 * normal number, it must also implement:
 * struct ExampleKey {.
     // This returns the i'th part of key.
     typedef ... key_part_t;
     key_part_t get_key_part(size_t i) const;
     size_t get_num_key_parts() const;
   }
   or else implement KeyTraits for these (see compare_record.hh)
 *
 * Note -- when using permutation functions, the algorithm needs
 * to be able to convert the key into a number. Therefore,
 * it must support assignement from a number to a key.
 */

struct return_key_criterion_tag { };
struct compare_criterion_tag { };

struct CompareSameCriterion {
   typedef compare_criterion_tag criterion_category;
   template<typename Record>
   int compare(Record a, Record b) const {
     return 0;
   }
};

#endif
