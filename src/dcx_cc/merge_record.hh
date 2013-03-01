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

  femto/src/dcx_cc/merge_record.hh
*/
#ifndef _MERGE_RECORD_HH_
#define _MERGE_RECORD_HH_

#include <cstdlib>
#include <iostream>
#include <string>
#include <sstream>
#include <cassert>
#include <limits>
#include "context.hh"
#include "dcover.hh"
#include "sorting_interface.hh"
#include <limits>

/*
** Sorting criterion for MergeRecords
*/

template <typename Record, typename Context>
struct MergeRecordSortingCriterion
{
  typedef return_key_parts_and_compare_criterion_tag criterion_category;
  typedef unsigned char key_part_t;
  static const bool sort_keys_ascending = Context::sort_ascending;
  static const size_t period = Dcover::period;

  static key_part_t get_key_part(const Context* ctx, const Record& r, size_t i)
  { 
    key_part_t* p = (key_part_t*) r.char_vals;
    return p[i];
  }

  static size_t get_num_key_parts(const Context* ctx, const Record& r) 
  {
    size_t char_vals_len = r.char_vals_len;
    return char_vals_len*sizeof(typename Record::character_t); 
  }

  // Each compare_after_key(a,b) returns -1 if a < b; 1 if a > b, or 0 if a==b.
  static int compare_after_key(const Context* ctx, const Record& a, const Record& b) 
  {
    const size_t period = ctx->dcx->period;
    size_t k, kp, jp, ip, L, d;
    int ans;

    typename Record::offset_t::register_type i = a.i, j = b.i;

    // if a and b are from the same sample, but their key lengths differ this is a sign that
    //  one occurs at the end of the string (and "ran out" of key material, thus the length difference)
    if ( (i%period) == (j%period) )
    {
	if (get_num_key_parts(ctx,a) < get_num_key_parts(ctx,b)) return -1;
	if (get_num_key_parts(ctx,a) > get_num_key_parts(ctx,b)) return 1;
    }

    // we've already compared by the characters in char_vals (these are used as the key for the record)
    //  now, we will compare using the ranks.

    // find L s.t. i+L and j+L are both in the cover
    L = ctx->dcx->get_offset(i,j);

    // look up S[i+L] and S[j+L]
    //  find distance i' between i and i+L, and distance j' between j and j+L
    //  with sample[i] = k and sample[i+L]=k', then the distance is k'-k
    //  because sample[i] = index of i in the array of cover values
    d = ctx->dcx->get_distance(i%period);
    k = ctx->dcx->sample[(i+d)%period];
    kp = ctx->dcx->sample[(i+L)%period];
    ip = ctx->dcx->cmod(kp-k);
    d = ctx->dcx->get_distance(j%period);
    k = ctx->dcx->sample[(j+d)%period];
    kp = ctx->dcx->sample[(j+L)%period];
    jp = ctx->dcx->cmod(kp-k);
 
    assert(ip < ctx->dcx->cover_size);
    assert(jp < ctx->dcx->cover_size);

    // return the comparison of S[i+L] and S[j+L]
    typename Record::rank_t::register_type ar = a.rank_vals[ip], br = b.rank_vals[jp];
    ans = (ar < br) ? -1 :(ar > br) ? 1 : 0;

    if (ctx->DEBUG>4) 	std::cout << "comparing: " << i << "/" << j 
				  << "\t" << "ip=" << ip << ";jp=" << jp << ";ans=" << (int) ans << std::endl;

    // because of padding, we have the rare occasion of a tie. that is, both records have 0 rank (we padded with 0)
    //  these ties are simple to break by looking at the offsets
    if (ans==0) ans = (i < j) ? 1 : (i > j) ? -1 : 0;
    return ans;
  }
};



/*
** This class creates variable length records for sample and non-sample entries
** in-lining each record with enough data to merge without look-ups
*/

/*
** The following information is needed for merging:
**
** Ex: when Dcover::period = 3 and dcx->cover = {1,2}
** the records will look like
** 	{0, S[1], S[2], T[0..1]}
** 	{1, S[1], S[2], T[1]}
**	{2, S[2], S[4], T[2..3]}
**	{3, S[4], S[5], T[3..4]} ...
** Ex: when Dcover::period = 7 and dcx->cover = {0,1,3}
** the records will look like
**	{0, S[0], S[1], S[3], T[0..2]}
**	{1, S[1], S[3], S[7], T[1...6]}
**	{2, S[3], S[7], S[8], T[2..7]} ...
**
** So, the records will be 
**	i, 
**	followed by |cover| values of type N,
** 	and (if the last value is S[x]) this will be followed by x-i values of type T
*/

template <typename T, typename R, typename N, typename Ctx>
class MergeRecord
{
   private:
	typedef MergeRecord<T,R,N,Ctx> ThisRecord;
	typedef typename Dcover::period_t period_t;
	static const period_t period = Dcover::period;
	static const period_t cover_size = Dcover::cover_size;
	typedef typename R::register_type rank_rt;
	typedef typename N::register_type sptr_t;
	typedef typename T::register_type char_t;

   public:
	typedef R rank_t;
	typedef N offset_t;
	typedef T character_t;
	typedef typename Dcover::period_varint_t period_varint_t;
	typedef MergeRecordSortingCriterion<ThisRecord,Ctx> Sort;

	offset_t i;
	rank_t rank_vals[cover_size];
	character_t char_vals[period];
	static const period_varint_t rank_vals_len;	// # of chars of type N in rank_vals
	period_varint_t char_vals_len;			// # of chars of type T in char_vals

	MergeRecord(Ctx* ctx=NULL, sptr_t i=0, rank_rt* rv=NULL, period_t rvl=cover_size, char_t* cv=NULL, period_t cvl=period)
	 : i(i), char_vals_len(cvl)
	{

	   if (rv!=NULL)
	   {
	     assert(rvl == rank_vals_len);
	     for(size_t j=0; j<rvl; j++)
	     	rank_vals[j] = rv[j];
	   }

	   if (cv!=NULL)
	   {
	     for(size_t j=0; j<cvl; j++)
		char_vals[j] = cv[j];
	   }

	};

	template <typename C>
	size_t get_record_length(const C* ctx) const 
	{
	   period_t rvl = rank_vals_len, cvl = char_vals_len;
	   return (cvl*sizeof(character_t)+(1+rvl)*sizeof(rank_t));
	};

	void print_record() const
	{ std::cout << to_string<Ctx>(NULL) << std::endl; };

	template <typename C>
	std::string to_string(C* ctx) const
	{
	  period_t rvl = rank_vals_len, cvl = char_vals_len;
	  std::stringstream out;
	  size_t j=0;

	  out << i << ", \t";
	  while (j < rvl) { rank_rt r = rank_vals[j]; out << r << ", \t";  j++; }
	  out << "\t \t";
	  j=0;
	  while (j < cvl) { char_t c = char_vals[j]; out << c << ", \t";  j++; }
	  return out.str();
	};

	template <typename C>
	void encode(const C* ctx, unsigned char* p) const;

	template <typename C>
	void decode(const C* ctx, const unsigned char* p);

	static const MergeRecord<T,R,N,Ctx> min_value();
	static const MergeRecord<T,R,N,Ctx> max_value();
};


template <typename T, typename R, typename N, typename Ctx>
   const typename MergeRecord<T,R,N,Ctx>::period_varint_t
      MergeRecord<T,R,N,Ctx>::rank_vals_len = cover_size;

template <typename T, typename R, typename N, typename Ctx>
const MergeRecord<T,R,N,Ctx> MergeRecord<T,R,N,Ctx>::min_value() {
   rank_rt rv[cover_size];
   char_t cv[1];

   sptr_t n = std::numeric_limits<sptr_t>::max();
   for (size_t i=0; i<cover_size; i++)
      rv[i] = std::numeric_limits<rank_rt>::min();
   cv[0] = std::numeric_limits<char_t>::min();


   return MergeRecord<T,R,N,Ctx>(NULL, n, rv, cover_size, cv, 1);
}

template <typename T, typename R, typename N, typename Ctx>
const MergeRecord<T,R,N,Ctx> MergeRecord<T,R,N,Ctx>::max_value() {
   rank_rt rv[cover_size];
   char_t cv[period];

   sptr_t n = std::numeric_limits<sptr_t>::min();
   for (size_t i=0; i<cover_size; i++)
      rv[i] = std::numeric_limits<rank_rt>::max();
   for (size_t i=0; i<period; i++)
      cv[i] = std::numeric_limits<char_t>::max();
   
   return MergeRecord<T,R,N,Ctx>(NULL, n, rv, cover_size, cv, period);
}


//write to memory
template <typename T, typename R, typename N, typename Ctx> template <typename C>
void MergeRecord<T,R,N,Ctx>::encode(const C* ctx, unsigned char* p) const {
   period_t rvl = rank_vals_len, cvl = char_vals_len; 

   memcpy(p, &i, sizeof(offset_t));
   p+=sizeof(offset_t);
   memcpy(p, &rank_vals, rvl*sizeof(rank_t));
   p += rvl*sizeof(rank_t);
   memcpy(p, &char_vals, cvl*sizeof(character_t));
}

//read from memory
template <typename T, typename R, typename N, typename Ctx> template <typename C>
void MergeRecord<T,R,N,Ctx>::decode(const C* ctx, const unsigned char* p) {
   i = *(offset_t *) p;
   p += sizeof(offset_t);
   sptr_t _i = i;

   memcpy(&rank_vals, p, cover_size*sizeof(rank_t));
   p += cover_size*sizeof(rank_t);

   sptr_t cvl = ctx->dcx->edge[_i%period];		   // last element in rank_vals is S[i+x] where x = edge[i]
   assert(cvl <= period);
   if (ctx->n < _i + cvl) 	cvl -= (_i+cvl-ctx->n);	   // if close to the end, we may need less than edge[i] values
   assert(cvl <= period);
   memcpy(&char_vals, p, cvl*sizeof(character_t));
   char_vals_len = cvl;
}

#endif
