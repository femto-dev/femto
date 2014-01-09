/*
  (*) 2013-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/json.h
*/

#include "index_types.h"

#define MAX_JSON 16

// dst must have space for MAX_JSON per character.
int encode_ch_json(char* dst, int ch);
int encode_str_json(char* dst, const char* str);
// dst must have space for MAX_JSON characters
int encode_alpha_json(char* dst, alpha_t alpha);
void fprint_alpha_json(FILE* f, int len, alpha_t* pat);
void fprint_str_json(FILE* f, int len, const unsigned char* str);
void fprint_cstr_json(FILE* f, const char* str);
