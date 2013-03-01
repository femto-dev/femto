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

  femto/src/dcx_cc/create_tuples_node.hh
*/
#ifndef _GENERATE_INDEX_NODE_HH_
#define _GENERATE_INDEX_NODE_HH_

#include "pipelining.hh"
#include "dcover.hh"
#include <cstdlib>
#include <queue>

template <typename RecordType, typename TupleType, typename Ctx>
class create_tuples_node : public pipeline_node {
  public:
	create_tuples_node(Ctx* ctx, read_pipe* input, writer* writer, pipeline_node* parent)
	: pipeline_node(parent, "generate index node", ctx->print_timing),
	  ctx(ctx), input(input), writer(writer) {};

  private:
	Ctx* const ctx;
	read_pipe* const input;
	write_pipe* const output;
	static const size_t period = Dcover::period;
	virtual void run();
	TupleType make_record(std::deque<RecordType>* name, const typename Ctx::sptr_t offset);
};

template <typename RecordType, typename TupleType, typename Ctx>
void create_tuples_node<RecordType,TupleType,Ctx>::run()
{
   typename Ctx::sptr_t offset = 0;

   pipe_iterator<Ctx,RecordType> read(ctx,input);
   pipe_iterator<Ctx,RecordType> end;
   //pipe_back_inserter<Ctx,TupleType> write(ctx,output);
   std::deque<RecordType> tmp_char;

   // get "period" characters into queue, or as many as possible
   if(read!=end)
	for(size_t i=0; i<period; i++)
	{
	   RecordType c = *read;
	   tmp_char.push_back(c);
	   ++read;
	   if (read==end) 	break;
	}

   // do work for loop
   while (!tmp_char.empty())
   {
	//if offset is in Dcover, make a record for it
	if (ctx->dcx->get_distance(offset % ctx->dcx->period)==0)
	{
	   TupleType r = make_record(&tmp_char, offset);
	   writer.push_back(r);

	   if (ctx->DEBUG>3) r.print_record();
   	   else if (ctx->DEBUG>1) {
             if (tmp_char.size() < ctx->dcx->period) r.print_record();
             else if (offset < ctx->dcx->period) r.print_record();
             else if (0 == offset % 5) std::cout << ".";
           }
	}

	tmp_char.pop_front();

	++offset;

	// get next char
	if (read!=end)
	{
	   RecordType c = *read;
	   tmp_char.push_back(c);
	   ++read;
	}
   }

   writer.finish();
}


template <typename RecordType, typename TupleType, typename Ctx>
TupleType create_tuples_node<RecordType,TupleType,Ctx>::make_record(std::deque<RecordType>* q, const typename Ctx::sptr_t offset)
{
   typename Ctx::longname_t lname;
   size_t size = q->size();
   size_t len = (size < period)? size : period;

   // make up a record, using a (perhaps partially filled queue of RecordType)
   for(size_t i=0; i<len; i++)
	lname.value[i] = q->at(i).value;		//use the char_t to make longname_t
   lname.num = len;
   TupleType record(lname, offset);			//use the longname to make a tuple record
   return record;
}

#endif
