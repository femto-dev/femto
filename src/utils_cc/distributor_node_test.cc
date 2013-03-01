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

  femto/src/utils_cc/distributor_node_test.cc
*/
#include <cstdlib>
#include <cassert>
#include "example_record.hh"
#include "distributor_node.hh"

typedef distributor_node<Record> distributor;

int main(int argc, char **argv)
{
  unsigned char buf[1024];
  unsigned char key[1024];
  FILE *input = tmpfile();

  int num_records = 10000;
  size_t wrote;
  // Make up some records and store them in a file.
  // Make records with random keys and values counting up.
  for (int i = 0; i < num_records; i++) {
    int key_length = rand() % 10;       // in [0,10)
    for (int j = 0; j < key_length; j++) {
      int r = rand();
      if (r % 3 == 0) {
        key[j] = 'a' + rand() % (1 + 'z' - 'a');
      } else if (r % 3 == 1) {
        key[j] = 'A' + rand() % (1 + 'Z' - 'A');
      } else {
        key[j] = '0' + rand() % (1 + '9' - '0');
      }
    }
    Record r(key_length, key, i);
    // Write(encode) the record to buf.
    r.encode(buf);
    // Write the buffer containing a single record to the file.
    wrote = fwrite(buf, r.get_record_length(), 1, input);
    assert(wrote == 1);
  }

  rewind(input);

  // Then we'll copy that file to another file, printing out the records.
  buffered_pipe pipe0(512 * 1024, 2);

  vector < FILE * >output;
  vector < write_pipe * >output_pipes;
  vector < file_writer_node * >writers;
  for (size_t i = 0; i < distributor::num_bins; i++) {
    FILE *tmp = tmpfile();
    output.push_back(tmp);
    buffered_pipe *pipe1 = new buffered_pipe(512 * 1024, 2);
    output_pipes.push_back(pipe1);
    file_writer_node *writer = new file_writer_node(tmp, *pipe1);
    writers.push_back(writer);
  }

  file_reader_node reader(input, pipe0);
  distributor_node < Record > test(&pipe0, &output_pipes);

  cout << "Beginning distribution" << endl;

  // start the threads.
  reader.start();
  test.start();
  for (size_t i = 0; i < distributor::num_bins; i++)
    writers[i]->start();

  // wait for the threads to finish.
  reader.finish();
  test.finish();
  for (size_t i = 0; i < distributor::num_bins; i++)
    writers[i]->finish();

  cout << "Distribution completed. Cleaning up" << endl;

  for (size_t i = 0; i < distributor::num_bins; i++) {
    delete output_pipes[i];
    delete writers[i];
  }

  return 0;
}
