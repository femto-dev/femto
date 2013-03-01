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

  femto/src/dcx_cc/singleton_record.hh
*/
#ifndef _SINGLETON_RECORD_HH_
#define _SINGLETON_RECORD_HH_

#include <cstdlib>
#include <string.h>
#include <iostream>
#include <sstream>
#include <limits>
#include "context.hh"
#include "sorting_interface.hh"
#include <limits>


template <typename T, typename Record, typename Context>
struct SingletonRecordSorter
{
   typedef return_key_criterion_tag criterion_category;
   typedef typename Record::value_t key_t;
   static const bool sort_keys_ascending = Context::sort_ascending;
   static key_t get_key(const Context* ctx, const Record& r)
   {
      typename Record::value_t::register_type v = r.value;
      return v;
   }
};



template <typename T, typename Ctx>
class SingletonRecord
{
   private:
	typedef T packed_t;
	typedef typename T::register_type unpacked_t;
	typedef SingletonRecord<T,Ctx> ThisRecord;

   public:
	typedef packed_t value_t;
	typedef SingletonRecordSorter<T,ThisRecord,Ctx> Sort;

	value_t value;

	SingletonRecord(unpacked_t value = 0)
	: value(value) { };

	template <typename C>
	void encode(const C* ctx, unsigned char* p) const
	{
	   memcpy(p, &value, sizeof(packed_t));
	};

	template <typename C>
	void decode(const C* ctx, const unsigned char* p)
	{
	   value = *(packed_t*) p;
	};

	template <typename C>
	size_t get_record_length(const C* ctx) const	{ return sizeof(packed_t); };

	void print_record() const			{ std::cout << to_string<Ctx>(NULL) << std::endl; };

	template <typename C>
	std::string to_string(const C* ctx) const	{ std::ostringstream os;
							  unpacked_t v = value;
							  os << v;
							  return os.str(); };
	static const SingletonRecord<T,Ctx> min_value()
	{
	   unpacked_t mv = std::numeric_limits<unpacked_t>::min();
	   return SingletonRecord<T,Ctx>(mv);
	};

	static const SingletonRecord<T,Ctx> max_value()
	{
	   unpacked_t mv = std::numeric_limits<unpacked_t>::max();
	   return SingletonRecord<T,Ctx>(mv);
	};
};


#endif
