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

  femto/src/dcx_cc/naming_node.hh
*/
#ifndef _NAMING_NODE_HH_
#define _NAMING_NODE_HH_

#include "pipelining.hh"
#include "tuple_record.hh"

template <typename TupleType, typename NamedRecord, typename Ctx>
class naming_node : public pipeline_node {
  public:
	naming_node(Ctx* ctx, read_pipe* input, writer* writer, pipeline_node* parent)
	: pipeline_node(parent, "naming node", ctx->print_timing),
	  ctx(ctx), input(input), output(output), flag(0) {};

  private:
	Ctx* const ctx;
	read_pipe* const input;
	write_pipe* const output;
	int flag;			// 0 = dont know; 1 = need to iterate; -1 = don't need to
	virtual void run();
};

template <typename TupleType, typename NamedRecord, typename Ctx>
void naming_node<TupleType,NamedRecord,Ctx>::run()
{
   pipe_iterator<Ctx,TupleType> read(ctx, input);
   pipe_iterator<Ctx,TupleType> end;

   typename Ctx::longname_t name, name_tmp;
   typename Ctx::sptr_t value, counter = 0;
   typename Ctx::shortname_t::register_type shortname = start_rank;
   typename Ctx::shortname_t::register_type nextname = start_rank;
   bool first_iter = true;
   bool unique = true;


   while(read != end)
   {
	TupleType x = *read;
	name = x.first;
	value = x.second;

	// if names are the same, we don't increment shortname
	if (!first_iter && name==name_tmp)
           // same name!
           unique = false;
	else
	   ++shortname;

        ++nextname;
	
	NamedRecord r(shortname, value);
	writer.push_back(r);

	if (ctx->DEBUG>3) r.print_record();
	else if (ctx->DEBUG>1) {
          if (value < ctx->dcx->period) 		r.print_record();
	  else if (value % 5 == 0) 		std::cout<<".";
        }

	++counter;
	++read;

	name_tmp = name;
	first_iter = false;
   }
   writer.finish();

   ctx->need_to_iterate = !unique;
   ctx->n_for_iteration = counter;
}


#endif
