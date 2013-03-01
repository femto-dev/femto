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

  femto/src/utils_cc/print_records_node.hh
*/
#include <cassert>
#include <string>
#include "pipelining.hh"

template<typename Context, typename Record>
class print_records : public pipeline_node {
  private:
    Context* const ctx;
    read_pipe* input;
    write_pipe* output;
    std::string prefix;

    virtual void run() {
      try {
        pipe_iterator<Context,Record> reader(ctx, input);
        pipe_iterator<Context,Record> end;
        pipe_back_inserter<Context,Record> writer(ctx, output);

        while( reader != end ) {
          // Read a record.
          Record & r = *reader;
          // Print the record.
          std::cout << prefix << r.to_string(ctx) << std::endl;
          // Write the record.
          writer.push_back(r);
        }

        writer.finish();
      } catch ( ... ) {
        input->close_empty();
        output->close_full();
        throw;
      }
    }
  public:
    print_records(Context* ctx, read_pipe* input, write_pipe* output, std::string prefix)
      : ctx(ctx), input(input), output(output), prefix(prefix)
    {
    }
};

