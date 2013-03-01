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

  femto/src/mpi/mpi_utils_test.cc
*/
#include "mpi_utils.hh"

extern "C" {
#include "timing.h"
}

#define VERBOSE 0
#define PROGRESS 0
// Repeat tests this many times... to look for race/deadlock.
#define REPEATS 5

typedef uint64_t test_record_t;

long gb = 1024L*1024L*1024L;
long mb = 1024L*1024L;

struct test_producer : public pipeline_node {
  std::vector<write_pipe*>* output;
  bin_num n_bins;
  size_t n;
  bool dontclose;
  std::vector<size_t> num_written;


  virtual void run()
  {
    std::vector<pipe_back_inserter<test_record_t>*> writers;

    for( bin_num b = 0; b < n_bins; b++ ) {
      pipe_back_inserter<test_record_t>* writer = NULL;
      writer = new pipe_back_inserter<test_record_t>((*output)[b], dontclose);
      writers.push_back(writer);
      num_written.push_back(0);
    }

    for( size_t i = 0; i < n; i++ ) {
      //int num = rand();
      test_record_t num = i ^ rand();
      bin_num dst = num % n_bins;
      assert(dst>=0);

      if( VERBOSE && n < 1000 ) printf("producer %p producing %lx\n", this, (long) num);
      writers[dst]->push_back(num);
      num_written[dst]++;
    }

    for( bin_num b = 0; b < n_bins; b++ ) {
      writers[b]->finish();
      delete writers[b];
    }

    if( VERBOSE ) printf("test producer finished\n");
  }

  test_producer(std::vector<write_pipe*>* output, bin_num n_bins, size_t n, bool dontclose)
    : output(output), n_bins(n_bins), n(n), dontclose(dontclose), num_written()
  {
  }
};

struct parallel_producers: public pipeline_node {
  size_t nproc;
  std::vector<write_pipe*>* output;
  bin_num n_bins;
  size_t n;
  std::vector<size_t> num_written;

  parallel_producers(size_t nproc, std::vector<write_pipe*>* output, bin_num n_bins, size_t n)
    : nproc(nproc), output(output), n_bins(n_bins), n(n), num_written()
  {
  }

  virtual void run()
  {
    std::vector<test_producer*> producers;
    for( size_t i = 0; i < nproc; i++ ) {
      producers.push_back(new test_producer(output, n_bins, n, true));
      producers[i]->start();
    }

    for( size_t i = 0; i < nproc; i++ ) {
      producers[i]->finish();
    }

    for( bin_num b = 0; b < n_bins; b++ ) {
      num_written.push_back(0);
    }

    // Add up the totals.
    for( size_t i = 0; i < nproc; i++ ) {
      for( bin_num b = 0; b < n_bins; b++ ) {
        num_written[b] += producers[i]->num_written[b];
      }
    }

    // Close the output pipes.
    for( size_t i = 0; i < (*output).size(); i++ ) {
      (*output)[i]->close_full();
    }


    for( size_t i = 0; i < nproc; i++ ) {
      delete producers[i];
    }

    if( VERBOSE ) printf("parallel_producer finished\n");
  }
};

struct test_consumer : public pipeline_node {
  read_pipe* input;
  bin_num bin;
  bin_num n_bins;
  size_t n;
  size_t num_read;


  virtual void run()
  {
    pipe_iterator<test_record_t> read(input);
    pipe_iterator<test_record_t> end;

    while( read != end ) {
      test_record_t num = *read;
      if( VERBOSE && n < 1000 ) printf("consumer %p consuming %lx\n", this, (unsigned long int) num);
      assert((num%n_bins) == (test_record_t) bin);
      num_read++;
      ++read;
    }
    read.finish();
    if ( VERBOSE ) printf("consumer finished\n");
  }

  test_consumer(read_pipe* input, bin_num bin, bin_num n_bins, size_t n)
    : input(input), bin(bin), n_bins(n_bins), n(n), num_read(0)
  {
  }
};

void test_producer_consumer_core(MPI_handler & handler, size_t n, std::vector<int> & producer_locations, std::vector<int> & consumer_locations, bool timing=false)
{
  if( VERBOSE ) printf("Test started\n");

  bin_num n_bins = consumer_locations.size();

  MPI_pipe_group group(&handler, "test group", consumer_locations);

  std::vector<write_pipe*> producer_output;
  std::vector<test_consumer*> consumers;

  size_t n_local_producers=0;
  for( size_t i = 0; i < producer_locations.size(); i++ ) {
    if( producer_locations[i] == group.iproc ) n_local_producers++;
  }

  // Now create consumers for the ones we own.
  for( bin_num b = 0; b < n_bins; b++ ) {
    producer_output.push_back( group.bins[b].send_pipe );
    if( consumer_locations[b] == group.iproc ) {
      consumers.push_back( new test_consumer( group.bins[b].recv_pipe, b, n_bins, n) );
    } else {
      consumers.push_back( NULL );
    }
  }

  parallel_producers producers(n_local_producers, &producer_output, n_bins, n);

  group.barrier();

  start_clock();

  // Now start everything.
  for( size_t i = 0; i < consumers.size(); i++ ) {
    if( consumers[i] ) consumers[i]->start();
  }
  producers.start();

  // Handle MPI stuff.
  handler.add_group(&group);
  handler.work();
  handler.remove_group(&group);

  for( size_t i = 0; i < consumers.size(); i++ ) {
    if( consumers[i] ) consumers[i]->finish();
  }
  producers.finish();

  group.barrier();

  stop_clock();
  if( handler.iproc == 0 && timing ) {
    print_timings("MB all-to-all", n*sizeof(test_record_t)*producer_locations.size()/((double)mb));
  }

  // Do communication to check that the number read on each worker
  // matches the number read on each producer.

  std::vector<long long int> produced_by_bin;
  std::vector<long long int> total_produced_by_bin;
  std::vector<long long int> consumed_by_bin;
  std::vector<long long int> total_consumed_by_bin;
  consumed_by_bin.reserve(n_bins);
  total_consumed_by_bin.reserve(n_bins);
  produced_by_bin.reserve(n_bins);
  total_produced_by_bin.reserve(n_bins);
  assert(consumers.size() == (size_t) n_bins);
  for( bin_num b = 0; b < n_bins; b++ ) {
    if( consumers[b] ) {
      consumed_by_bin.push_back(consumers[b]->num_read);
    } else {
      consumed_by_bin.push_back(0);
    }
    total_consumed_by_bin.push_back(0);
    produced_by_bin.push_back(0);
    total_produced_by_bin.push_back(0);
  }

  for( bin_num b = 0; b < n_bins; b++ ) {
    produced_by_bin[b] += producers.num_written[b];
  }

#ifdef HAVE_MPI_H
  MPI_Allreduce(&consumed_by_bin[0], &total_consumed_by_bin[0],
                consumed_by_bin.size(), MPI_LONG_LONG_INT,
                MPI_SUM, group.comm);
  MPI_Allreduce(&produced_by_bin[0], &total_produced_by_bin[0],
                produced_by_bin.size(), MPI_LONG_LONG_INT,
                MPI_SUM, group.comm);
#else
  total_consumed_by_bin = consumed_by_bin;
  total_produced_by_bin = produced_by_bin;
#endif

  if( group.iproc == 0 ) {
    // Check that total_consumed_by_bin == total_produced_by_bin
    for( bin_num b = 0; b < n_bins; b++ ) {
      assert( total_consumed_by_bin[b] == total_produced_by_bin[b] );
    }
  }

  for( size_t i = 0; i < consumers.size(); i++ ) {
    if( consumers[i] ) delete consumers[i];
  }
  group.barrier();
}

void test_producer_consumer(MPI_handler & handler, size_t n, std::vector<int> & producer_locations, std::vector<int> & consumer_locations, bool timing=false)
{
  for( int i = 0; i < REPEATS; i++ ) {
    test_producer_consumer_core(handler, n, producer_locations, consumer_locations, timing);
  }
}

void test_mpi_comms(int n, int n_producers, int n_bins)
{
  MPI_handler handler(DEFAULT_TILE_SIZE*DEFAULT_TILES_PER_IO_GROUP, 2*n_producers,
                      DEFAULT_COMM);

  std::vector<int> producer_locations;
  std::vector<int> consumer_locations;

  if( handler.iproc == 0 && PROGRESS ) printf("-------------  test_mpi_comms(n=%i,n_producers=%i,n_bins=%i)\n", n, n_producers, n_bins);

  // Try all on node 0.
  if( handler.iproc == 0 && PROGRESS ) printf("-------------  all on node 0\n");
  producer_locations.clear();
  consumer_locations.clear();
  for( int i = 0; i < n_producers; i++ ) {
    producer_locations.push_back(0);
  }
  for( int i = 0; i < n_bins; i++ ) {
    consumer_locations.push_back(0);
  }
  test_producer_consumer(handler, n, producer_locations, consumer_locations);
  
  // Try round-robin distributed.
  if( handler.iproc == 0 && PROGRESS ) printf("-------------  round robin\n");
  producer_locations.clear();
  consumer_locations.clear();
  for( int i = 0; i < n_producers; i++ ) {
    producer_locations.push_back(i % handler.nproc);
  }
  for( int i = 0; i < n_bins; i++ ) {
    consumer_locations.push_back(i % handler.nproc);
  }
  test_producer_consumer(handler, n, producer_locations, consumer_locations);

  // If we have at least 2 nodes,
  // try producers on 0 and consumers on 1.
  if( handler.nproc >= 2 ) {
    if( handler.iproc == 0 && PROGRESS ) printf("-------------  0 to 1\n");
    producer_locations.clear();
    consumer_locations.clear();
    for( int i = 0; i < n_producers; i++ ) {
      producer_locations.push_back(0);
    }
    for( int i = 0; i < n_bins; i++ ) {
      consumer_locations.push_back(1);
    }
    test_producer_consumer(handler, n, producer_locations, consumer_locations);
  }
  
}

int main(int argc, char** argv)
{
  MPI_handler::init_MPI(&argc, &argv);
 
  size_t big = 2*DEFAULT_TILE_SIZE + 10;


  if( argc == 1 ) {
    test_mpi_comms(1, 1, 1);
    test_mpi_comms(10, 1, 1);
    test_mpi_comms(100, 1, 1);
    test_mpi_comms(1, 2, 1);
    test_mpi_comms(10, 2, 1);
    test_mpi_comms(100, 2, 1);
    test_mpi_comms(1, 1, 2);
    test_mpi_comms(10, 1, 2);
    test_mpi_comms(100, 1, 2);
    test_mpi_comms(1, 2, 2);
    test_mpi_comms(10, 2, 2);
    test_mpi_comms(100, 2, 2);
    test_mpi_comms(1, 1, 4);
    test_mpi_comms(10, 1, 4);
    test_mpi_comms(100, 1, 4);
    test_mpi_comms(1, 4, 1);
    test_mpi_comms(10, 4, 1);
    test_mpi_comms(100, 4, 1);
    test_mpi_comms(1, 4, 4);
    test_mpi_comms(10, 4, 4);
    test_mpi_comms(100, 4, 4);
    test_mpi_comms(1, 1, 10);
    test_mpi_comms(10, 1, 10);
    test_mpi_comms(100, 1, 10);

    test_mpi_comms(big, 1, 1);
    test_mpi_comms(big, 2, 1);
    test_mpi_comms(big, 1, 2);
    test_mpi_comms(big, 2, 2);
    test_mpi_comms(big, 1, 4);
    test_mpi_comms(big, 4, 1);
    test_mpi_comms(big, 4, 4);
    test_mpi_comms(big, 1, 10);
    test_mpi_comms(big, 1, 20);


    printf("All tests pass\n");
  } else {

    MPI_handler handler(DEFAULT_TILE_SIZE*DEFAULT_TILES_PER_IO_GROUP, 2, DEFAULT_COMM);
    std::vector<int> producer_locations;
    std::vector<int> consumer_locations;
    int num_bins_per_proc = 4;
    double num_gb = atof(argv[1]);
    int test = atoi(argv[2]);
    size_t n = num_gb * gb;

    if( test == 0 ) {
      printf("Running buffered pipe performance test\n");
      if( handler.iproc == 0 ) {
        buffered_pipe pipe(DEFAULT_TILE_SIZE*DEFAULT_TILES_PER_IO_GROUP, 2);
        std::vector<write_pipe*> output;
        output.push_back(&pipe);
        test_producer producer( &output, 1, n, false );
        test_consumer consumer( &pipe, 0, 1, n );

        start_clock();
        producer.start();
        consumer.start();
        consumer.finish();
        producer.finish();
        stop_clock();
        print_timings("MB one-to-one", n*sizeof(test_record_t)/((double)mb));
      }
    } else if( test == 1 ) {

      printf("Running all-to-all performance test, %lf gb, %i bins per proc, %i procs\n",
              num_gb, num_bins_per_proc, handler.nproc);

      for( int i = 0; i < handler.nproc; i++ ) {
        producer_locations.push_back(i);
      }
      for( int i = 0; i < handler.nproc; i++ ) {
        for( int j = 0; j < num_bins_per_proc; j++ ) {
          consumer_locations.push_back( i );
        }
      }
      test_producer_consumer_core(handler, n, producer_locations, consumer_locations, true);
    } else {
      printf("Unknown test %i\n", test);
    }
  }

  MPI_handler::finalize_MPI();

  return 0;
}
