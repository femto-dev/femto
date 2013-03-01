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

  femto/src/dcx_cc/tuple_record.hh
*/
#ifndef _TUPLE_RECORD_HH_
#define _TUPLE_RECORD_HH_

#include <cstdlib>
#include <string.h>
#include <iostream>
#include <sstream>
#include <limits>
#include "context.hh"
#include "dcover.hh"
#include "sorting_interface.hh"
#include "error.hh"
#include <limits>

/*
** A bunch of sorting criteria useful for TupleRecords
*/

template <typename T, typename N, typename Record, typename Context>
struct TupleRecordByFirstSorter
{
   typedef return_key_criterion_tag criterion_category;
   typedef typename Record::first_t key_t;
   static const bool sort_keys_ascending = Context::sort_ascending;
   static key_t get_key(const Context* ctx, const Record& r)
   {
	const typename Record::first_t::register_type f = r.first;
	return f;
   }
};

template <typename T, typename N, typename Record, typename Context>
struct TupleRecordByFirstSorter<longname<T>, N, Record, Context>
{
   typedef compare_criterion_tag criterion_category;
   static int compare(const Context* ctx, const Record& a, const Record& b)
   {	//these have been overloaded for longname<T>
	if (a.first == b.first) return 0;
	return (a.first > b.first)? 1:-1;
   }
};

template <typename T, typename N, typename Record, typename Context>
struct TupleRecordBySecondSorter
{
   typedef return_key_criterion_tag criterion_category;
   typedef typename Record::second_t key_t;
   static const bool sort_keys_ascending = Context::sort_ascending;
   static key_t get_key(const Context* ctx, const Record& r)
   {
	const typename Record::second_t::register_type s = r.second;
	return s;
   }
};

template <typename T, typename N, typename Record, typename Context>
struct TupleRecordByModSorter
{
   typedef compare_criterion_tag criterion_category;
   static int compare(const Context* ctx, const Record& a, const Record& b)
   {
	const typename Record::second_t::register_type x = a.second;
	const typename Record::second_t::register_type y = b.second;
	const int z = Dcover::period;
	if (x % z == y % z) return (x / z > y / z)? 1:-1;
	return (x % z > y % z)? 1:-1;
   }
};


/*
** This defines a very simple Record which is a tuple
** Use Ctx to determine T's "packed" type (Ctx::character_t) and N's "packed" type (Ctx::offset_t)
*/

template <typename T, typename N, typename Ctx>
class TupleRecord
{
   private:
	typedef typename T::register_type first_unpacked_t;
	typedef typename N::register_type second_unpacked_t;
	typedef TupleRecord<T,N,Ctx> ThisRecord;
   public:
	typedef T first_t;
	typedef N second_t;
	typedef TupleRecordByFirstSorter<T,N,ThisRecord,Ctx> ByFirst;
	typedef TupleRecordBySecondSorter<T,N,ThisRecord,Ctx> BySecond;
	typedef TupleRecordByModSorter<T,N,ThisRecord,Ctx> ByMod;

	first_t first;
	second_t second;

	TupleRecord(first_t val1=0, second_t val2=0)
	 : first(val1), second(val2) {};

	template <typename C>
	size_t get_record_length(const C* ctx) const
	{  return sizeof(first_t) + sizeof(second_t); };

	void print_record() const
	{  std::cout << to_string<Ctx>(NULL) << std::endl; };

	template <typename C>
	std::string to_string(const C* ctx) const
	{
	   std::ostringstream os;
	   first_unpacked_t x = first;
	   second_unpacked_t y = second;
	   os << x << " " << y;
	   return os.str();
	};

	template <typename C>
	void encode(const C* ctx, unsigned char* p) const
	{
	   memcpy(p, &first, sizeof(first_t));
	   p += sizeof(first_t);
	   memcpy(p, &second, sizeof(second_t));
	};

	template <typename C>
	void decode(const C* ctx, const unsigned char* p)
	{
	   first = *(first_t *) p;
	   p += sizeof(first_t);
	   second = *(second_t*) p;
	};

	static const ThisRecord min_value()
	{
	   first_unpacked_t m_first = std::numeric_limits<first_unpacked_t>::min();
	   second_unpacked_t m_second = std::numeric_limits<second_unpacked_t>::min();
	   return ThisRecord(m_first, m_second);
	};

	static const ThisRecord max_value()
	{
	   first_unpacked_t m_first = std::numeric_limits<first_unpacked_t>::max();
	   second_unpacked_t m_second = std::numeric_limits<second_unpacked_t>::max();
	   return ThisRecord(m_first, m_second);
	};
};


/*
** Specialization of TupleRecord for longname<T>
*/
template <typename T, typename N, typename Ctx>
class TupleRecord<longname<T>,N,Ctx>
{
   private:
	typedef T character_t;
	typedef typename character_t::register_type char_t;
	typedef typename Dcover::period_varint_t period_varint_t;
	typedef typename Dcover::period_t period_t;
	typedef typename N::register_type second_unpacked_t;
	typedef TupleRecord<longname<T>,N,Ctx> ThisRecord;
   public:
	typedef longname<T> first_t;
	typedef N second_t;
	typedef TupleRecordByFirstSorter<longname<T>,N,ThisRecord,Ctx> ByFirst;
	typedef TupleRecordBySecondSorter<longname<T>,N,ThisRecord,Ctx> BySecond;
	typedef TupleRecordByModSorter<longname<T>,N,ThisRecord,Ctx> ByMod;

	longname<T> first;
	second_t second;

	TupleRecord(longname<T> val1=0, second_unpacked_t val2=0)
	 : first(val1), second(val2) { };

	template <typename C>
	size_t get_record_length(const C* ctx) const
	{
	  period_t num = first.num;
	  return sizeof(second_t) + sizeof(period_varint_t) + num*sizeof(character_t);
	};

	void print_record() const
	{  std::cout << to_string<Ctx>(NULL) << std::endl;  };

	template <typename C>
	std::string to_string(const C* ctx) const 
	{
	   std::ostringstream os;
	   second_unpacked_t y = second;
   	   os << first << " " << y;
  	   return os.str();
	};

	template <typename C>
	void encode(const C* ctx, unsigned char* p) const
	{
	   memcpy(p, &(first.num), sizeof(period_varint_t));
	   p += sizeof(period_varint_t);

	   size_t num = first.num;
	   for(size_t i=0; i<num; i++)
	   {
	      memcpy(p, &(first.value[i]), sizeof(character_t));
	      p += sizeof(character_t);
	   }
	   memcpy(p, &second, sizeof(second_t));
	};

	template <typename C>
	void decode(const C* ctx, const unsigned char* p)
	{
	   first.num = *(period_varint_t*) p;
	   period_t num = first.num;
	   p += sizeof(period_varint_t);
	   for (size_t i=0; i<num; i++)
	   {
	      first.value[i] = *(character_t*) p;
	      p+= sizeof(character_t);
	   }
	   second = *(second_t*) p;
	};

	static const ThisRecord min_value()
	{
	   period_t num = std::numeric_limits<period_t>::min();
	   char_t cv[1];
	   cv[0] = std::numeric_limits<char_t>::min();
	   first_t m_first(cv,num);

	   second_unpacked_t m_second = std::numeric_limits<second_unpacked_t>::min();
	   return ThisRecord(m_first, m_second);
	};

	static const ThisRecord max_value()
	{
	   period_t num = Dcover::period;
	   char_t cv[num];
	   for (size_t i=0; i<num; i++)
	      cv[i] = std::numeric_limits<char_t>::max();
	   first_t m_first(cv,num);

	   second_unpacked_t m_second = std::numeric_limits<second_unpacked_t>::max();
	   return ThisRecord(m_first, m_second);
	};
};


#endif
