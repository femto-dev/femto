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

  femto/src/utils/suffix_sort.c
*/


#include <assert.h>

#include "error.h"
#include "suffix_sort.h"

static inline
string_t get_string_from_S_core(const size_t p_bytes_per_pointer, const void* context, const void* data_ptr)
{
  suffix_context_t* ctx = (suffix_context_t*) context;
  sptr_t byte_offset;

  // data_ptr is a location in the S array.
  byte_offset = get_S_with_ptr(p_bytes_per_pointer, data_ptr);
  return ctx->T+byte_offset;
}
string_t get_string_from_S_1(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(1, context, data_ptr);
}
string_t get_string_from_S_2(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(2, context, data_ptr);
}
string_t get_string_from_S_3(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(3, context, data_ptr);
}
string_t get_string_from_S_4(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(4, context, data_ptr);
}
string_t get_string_from_S_5(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(5, context, data_ptr);
}
string_t get_string_from_S_6(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(6, context, data_ptr);
}
string_t get_string_from_S_7(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(7, context, data_ptr);
}
string_t get_string_from_S_8(const void* context, const void* data_ptr)
{
  return get_string_from_S_core(8, context, data_ptr);
}
string_t get_string_from_S_x(const void* context, const void* data_ptr)
{
  suffix_context_t* ctx = (suffix_context_t*) context;
  return get_string_from_S_core(ctx->bytes_per_pointer, context, data_ptr);
}

get_string_fun_t get_string_getter(suffix_context_t* p)
{
  switch(p->bytes_per_pointer) {
    case 1:
      return get_string_from_S_1;
    case 2:
      return get_string_from_S_2;
    case 3:
      return get_string_from_S_3;
    case 4:
      return get_string_from_S_4;
    case 5:
      return get_string_from_S_5;
    case 6:
      return get_string_from_S_6;
    case 7:
      return get_string_from_S_7;
    case 8:
      return get_string_from_S_8;
    default:
      return get_string_from_S_x;
  }
}

// string getter that handles un-complementing a value in S
// if it's negative..
static inline
string_t get_string_from_S_comp_core(const size_t p_bytes_per_pointer, const void* context, const void* data_ptr)
{
  suffix_context_t* ctx = (suffix_context_t*) context;
  sptr_t byte_offset;

  // data_ptr is a location in the S array.
  byte_offset = get_S_with_ptr(p_bytes_per_pointer, data_ptr);
  byte_offset = (byte_offset<0)?(~byte_offset):(byte_offset);
  return ctx->T+byte_offset;
}
string_t get_string_from_S_comp_1(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(1, context, data_ptr);
}
string_t get_string_from_S_comp_2(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(2, context, data_ptr);
}
string_t get_string_from_S_comp_3(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(3, context, data_ptr);
}
string_t get_string_from_S_comp_4(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(4, context, data_ptr);
}
string_t get_string_from_S_comp_5(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(5, context, data_ptr);
}
string_t get_string_from_S_comp_6(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(6, context, data_ptr);
}
string_t get_string_from_S_comp_7(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(7, context, data_ptr);
}
string_t get_string_from_S_comp_8(const void* context, const void* data_ptr)
{
  return get_string_from_S_comp_core(8, context, data_ptr);
}
string_t get_string_from_S_comp_x(const void* context, const void* data_ptr)
{
  suffix_context_t* ctx = (suffix_context_t*) context;
  return get_string_from_S_comp_core(ctx->bytes_per_pointer, context, data_ptr);
}

get_string_fun_t get_string_getter_comp(suffix_context_t* p)
{
  switch(p->bytes_per_pointer) {
    case 1:
      return get_string_from_S_comp_1;
    case 2:
      return get_string_from_S_comp_2;
    case 3:
      return get_string_from_S_comp_3;
    case 4:
      return get_string_from_S_comp_4;
    case 5:
      return get_string_from_S_comp_5;
    case 6:
      return get_string_from_S_comp_6;
    case 7:
      return get_string_from_S_comp_7;
    case 8:
      return get_string_from_S_comp_8;
    default:
      return get_string_from_S_comp_x;
  }
}


void print_suffixes(suffix_sorting_problem_t* p, int len)
{
  suffix_context_t ctx;
  string_sort_params_t params = {0};

  ctx.T = p->T;
  ctx.bytes_per_pointer = p->bytes_per_pointer;
  params.context = &ctx;
  params.base = p->S;
  params.n_memb = p->n;
  params.memb_size = p->bytes_per_pointer;
  params.str_len = len*p->bytes_per_character;
  params.same_depth = 0;
  params.get_string = get_string_getter(&ctx);
  params.compare = NULL;

  print_strings(&params, params.base, params.n_memb);
}

error_t prepare_problem(suffix_sorting_problem_t* p)
{
  if( p->n < 0 ) return ERR_PARAM;
  if( p->T == NULL ) return ERR_PARAM;
  if( p->S != NULL ) return ERR_PARAM;
  if( p->bytes_per_character <= 0 ) return ERR_PARAM;

  // set t_size
  p->t_size = p->n*p->bytes_per_character;
  // make sure to set p->bytes_per_pointer
  if( p->bytes_per_pointer == 0 ) {
    p->bytes_per_pointer = pointer_bytes_needed_for(p->t_size + p->t_padding_size);
  }
  // set s_size
  p->s_size = p->n*p->bytes_per_pointer;

  if( p->max_char == 0 ) {
    uint64_t tmp = 1;
    tmp <<= 8*p->bytes_per_character - 1;
    tmp += tmp - 1;
    p->max_char = tmp;
  }
  return ERR_NOERR;
}

