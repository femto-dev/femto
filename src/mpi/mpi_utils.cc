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

  femto/src/mpi/mpi_utils.cc
*/
#include "mpi_utils.hh"
#include "utils.hh"
#include "file_pipe_context.hh"
#include "file_pipe.hh"

#include <cassert>
#include <climits>

#include "error.hh"
#include "pipelining.hh"

#define DEBUG_MPI 0
#define USE_GREQUEST 1

// All nodes must get to copy_file at the same time.
// We attempt to detect if we're on a shared filesystem..
void mpi_copy_file(My_comm in_comm, const std::string & from_fname, int from_proc, const std::string & to_fname, int to_proc, bool move_not_copy)
{
  int iproc=0;

  mpi_barrier(in_comm);

#ifdef HAVE_MPI_H
  int nproc=1;

  int rc;
  My_comm comm;

  rc = MPI_Comm_dup(in_comm, &comm);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_dup failed"));

  // Set the rank and number of processes in this communicator.
  rc = MPI_Comm_size(comm,&nproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_size failed"));
  rc = MPI_Comm_rank(comm,&iproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_rank failed"));

  assert( to_proc >= 0 );
  assert( to_proc < nproc );
  assert( from_proc >= 0 );
  assert( from_proc < nproc );
#endif 


  if( from_proc == to_proc ) {
    /*printf("%i in local mpi_copy_file(from %s on proc %i   to %s on proc %i move=%i)\n",
           iproc, from_fname.c_str(), from_proc,
           to_fname.c_str(), to_proc,
           move_not_copy);
           */

    if( iproc == from_proc ) {
      // Copy the file
      copy_file(from_fname, to_fname, move_not_copy);
    }

    goto done;
  }

  /*
  printf("%i in remote mpi_copy_file(from %s on proc %i   to %s on proc %i move=%i)\n",
         iproc, from_fname.c_str(), from_proc,
         to_fname.c_str(), to_proc,
         move_not_copy);
         */


#ifdef HAVE_MPI_H
  if( iproc == to_proc || iproc == from_proc ) {
    // Transmit the file size.
    int s_reg = 0;
    int s_fsid = 1;
    int s_isnet = 2;
    int s_len = 3;
    int status_len = 4;
    long long int status[status_len];
    long long int other_status[status_len];
    error_t err;
    int isnet = -1;
    bool assume_shared = false;

    for( int i = 0; i < status_len; i++ ) {
      status[i] = other_status[i] = -1;
    }

    // status[0] is 1 if regular file exists
    // status[1] is fsid
    // status[2] is 1 if local fs
    // status[3] is file length
    
    {
      bool exists = false;
      bool regular = false;
      off_t len = 0;

      stat_file(from_fname, exists, regular, len);

      if( ! exists || ! regular ) status[s_reg] = 0;
      else {
        status[s_reg] = 1;

        // file has a size..
        status[s_len] = len;

        err = is_netfs(from_fname.c_str(), &isnet);
        if( err ) throw error(err);

        unsigned long fsid = 0;
        err = get_fsid(from_fname.c_str(), &fsid);
        if( err ) throw error(err);

        /*printf("iproc=%i isnet %s is %i fsid is %lu\n",
               iproc, from_fname.c_str(), isnet, fsid);
               */

        status[s_isnet] = isnet;

        status[s_fsid] = fsid;
      }
    }

    /*printf("iproc=%i status=%lli,%lli\n",
           iproc,
           status[0], status[1]);
           */


    {
      MPI_Status mpi_status;

      if( iproc == from_proc ) {
        // send file_size on from_proc to to_proc.
        rc = MPI_Sendrecv(status, status_len, MPI_LONG_LONG_INT,
                          to_proc, 0,
                          other_status, status_len, MPI_LONG_LONG_INT,
                          MPI_ANY_SOURCE, MPI_ANY_TAG,
                          comm, &mpi_status);
        if( rc ) throw error(ERR_IO_STR("MPI_Sendrecv failed"));
      }
      if( iproc == to_proc ) {
        // send file_size on to_proc to from_proc.
        rc = MPI_Sendrecv(status, status_len, MPI_LONG_LONG_INT,
                          from_proc, 0,
                          other_status, status_len, MPI_LONG_LONG_INT,
                          MPI_ANY_SOURCE, MPI_ANY_TAG,
                          comm, &mpi_status);
        if( rc ) throw error(ERR_IO_STR("MPI_Sendrecv failed"));
      }
    }

    /*printf("iproc=%i status=%lli,%lli,%lli,%lli other_status=%lli,%lli,%lli,%lli\n", 
         iproc,
         status[0], status[1], status[2], status[3],
         other_status[0], other_status[1], other_status[2], other_status[3] );
         */


    assume_shared = false;
    if( status[s_reg] && other_status[s_reg] ) {
      // File exists in both locations.
      
      if( status[s_len] == other_status[s_len] ) {
        // the file lengths are the same on both!
        
        if( status[s_fsid] == other_status[s_fsid] ) {
          // same filesystem! 
          assume_shared = true;
        } else {
          // Is one of them a network share?
          if( status[s_isnet] == 1 || other_status[s_isnet] == 1 ) {
            assume_shared = true;
          } else {
            assume_shared = false;
          }
        }
      }
    }

    if( assume_shared ) {
      // status[0] is OK for net on both
      // file size is same on both
      
      //printf("iproc=%i calling local file copy\n", iproc);
      
      if( iproc == from_proc ) {
        // OK - we believe we've detected a network share.
        copy_file(from_fname, to_fname, move_not_copy);
      }
    } else {

      long long int file_length = -1;
      if( iproc == to_proc ) {
        file_length = other_status[s_len];
      }
      if( iproc == from_proc ) {
        file_length = status[s_len];
      }

      //printf("iproc=%i doing mpi copy of %lli bytes\n", iproc, file_length);
      
      if( file_length < 0 ) {
        printf("File length <0 %lli in copy from %s on %i to %s on %i\n",
               file_length,
               from_fname.c_str(), from_proc,
               to_fname.c_str(), to_proc);

        assert( file_length >= 0 );
      }

      // OK, now send and receive.
      if( iproc == to_proc ) {
        long long int cur = 0;

        // Handle receiving end.
        file_pipe_context to_ctx = fctx_fixed_cached(to_fname);
        to_ctx.create = true;
        to_ctx.write = true;

        file_write_pipe_t write_pipe(to_ctx);

        while( 1 ) {
          if( cur >= file_length ) break;

          tile t_out = write_pipe.get_empty_tile();
          assert(t_out.data);

          MPI_Status mpi_status;
          int count;

          rc = MPI_Recv( t_out.data, t_out.max, MPI_BYTE, MPI_ANY_SOURCE, 0, comm, &mpi_status);
          if( rc ) throw error(ERR_IO_STR("MPI_Recv failed"));

          rc = MPI_Get_count(&mpi_status, MPI_BYTE, &count);
          if( rc ) throw error(ERR_IO_STR("MPI_Get_count failed"));

          //printf("iproc %i received %li bytes\n", iproc, (long int) count);
          
          t_out.len = count;
          assert( t_out.len <= t_out.max );

          cur += t_out.len;

          write_pipe.put_full_tile(t_out);
        }

        // The pipe should be done.
        {
          tile t = write_pipe.get_empty_tile();
          write_pipe.put_full_tile(t);
        }


        write_pipe.close_full();

        // Make sure the file length didn't change.
        assert(file_length == file_len(to_fname));
      }

      if( iproc == from_proc ) {
        long long int cur = 0;


        // Handle sending end.
        file_pipe_context from_ctx = fctx_fixed_cached(from_fname);

        file_read_pipe_t read_pipe(from_ctx);

        while( 1 ) {
          if( cur >= file_length ) break;

          tile t_in = read_pipe.get_full_tile();
          assert(t_in.data);

          //printf("iproc %i sending %li bytes\n", iproc, (long int) t_in.len);

          rc = MPI_Send( t_in.data, t_in.len, MPI_BYTE, to_proc, 0, comm);
          if( rc ) throw error(ERR_IO_STR("MPI_Send failed"));

          cur += t_in.len;

          read_pipe.put_empty_tile(t_in);
        }

        // The pipe should be done.
        {
          tile t = read_pipe.get_full_tile();
          assert(!t.data);
        }

        read_pipe.close_empty();

        // Make sure the file length didn't change.
        assert(file_length == file_len(from_fname));

        // Now, unlink the input file if necessary.
        if( move_not_copy ) {
          unlink_ifneeded(from_fname);
        }
      }
    }

  }
#endif


done:
#ifdef HAVE_MPI_H
  rc = MPI_Comm_free(&comm);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_free failed"));
#endif

  mpi_barrier(in_comm);

  return;
}

void mpi_move_file(My_comm in_comm, const std::string & from_fname, int from_proc, const std::string & to_fname, int to_proc)
{
  mpi_copy_file(in_comm, from_fname, from_proc, to_fname, to_proc, true);
}

// MPI HANDLER ----------------------------------------------
void MPI_handler::init_MPI(int* argc, char*** argv)
{
#ifdef HAVE_MPI_H
  int rc;
  int required = MPI_THREAD_MULTIPLE; // should be FUNNELED but OMP returns wrong...
  int provided = 0;
  rc = MPI_Init_thread(argc, argv, required, &provided);
  if( rc ) throw error(ERR_IO_STR("MPI_Init_thread failed"));

  if( provided != required ) {
    fprintf(stderr, "MPI_init_thread provides %i"
                    "(MPI_THREAD_SINGLE=%i, MPI_THREAD_FUNNELED=%i,"
                    "MPI_THREAD_SERIALIZED=%i, MPI_THREAD_MULTIPLE=%i)\n",
                    provided,
                    MPI_THREAD_SINGLE, MPI_THREAD_FUNNELED, 
                    MPI_THREAD_SERIALIZED, MPI_THREAD_MULTIPLE);

    throw error(ERR_IO_STR("MPI_Init_thread could not init for MPI_THREAD_FUNNELED; make sure you have an appropriately configured MPI implementation; e.g. OpenMPI configured with --with-threads=posix"));
  }
#endif
}

void mpi_barrier(My_comm in_comm)
{
#ifdef HAVE_MPI_H
  int rc;
  int in_nproc=1;
  rc = MPI_Comm_size(in_comm,&in_nproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_size failed"));
  if( in_nproc > 1 ) {
    rc = MPI_Barrier(in_comm);
    if( rc ) throw error(ERR_IO_STR("MPI_Barrier failed"));
  }
#endif
}


void MPI_handler::finalize_MPI()
{
#ifdef HAVE_MPI_H
  int rc;
  rc = MPI_Finalize();
  if( rc ) throw error(ERR_IO_STR("MPI_Finalize failed"));
#endif
}

int MPI_handler::get_nproc(My_comm comm)
{
  int nproc = 1;
#ifdef HAVE_MPI_H
  int rc;
  rc = MPI_Comm_size(comm,&nproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_size failed"));
#endif
  return nproc;
}

int MPI_handler::get_iproc(My_comm comm)
{
  int iproc = 0;
#ifdef HAVE_MPI_H
  int rc;
  rc = MPI_Comm_rank(comm,&iproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_rank failed"));
#endif
  return iproc;
}


MPI_handler::MPI_handler(size_t tile_size, size_t num_tiles_per_send_pipe, My_comm in_comm)
  : tile_size(tile_size), num_tiles_per_send_pipe(num_tiles_per_send_pipe), comm(NULL_COMM), iproc(0), nproc(1), pending(), groups_lock(), groups(), new_events(), signal_lock(),
#ifdef HAVE_MPI_H
  signal_request(NULL_REQUEST),
#else
  signal_cond(),
#endif
  signal_state(SIGNAL_NOTINITED)
{
#ifdef HAVE_MPI_H
  int rc;
  int provided = 0;

  // Verify that we have MPI_THREAD_MULTIPLE
  rc = MPI_Query_thread(&provided);
  if( rc ) throw error(ERR_IO_STR("MPI_Query_thread failed"));

  if( provided != MPI_THREAD_MULTIPLE ) {
    fprintf(stderr, "MPI_handler requires MPI_THREAD_MULTIPLE to be specified in MPI_Init_thread.\nTry changing a call to MPI_Init to MPI_handler::init_MPI()\n");
    throw error(ERR_IO_STR("MPI_handler requires MPI be initialized with MPI_THREAD_MULTIPLE; did you use MPI_handler::init_MPI()?"));
  }

  rc = MPI_Comm_dup(in_comm, &comm);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_dup failed"));
#endif

  // Get the rank and number of processes in this communicator.
  nproc = get_nproc(comm);
  iproc = get_iproc(comm);
}

MPI_handler::~MPI_handler()
{
#ifdef HAVE_MPI_H
  // Free the communicator we allocated.
  if( comm != MPI_COMM_NULL ) {
    int rc = MPI_Comm_free(&comm);
    if( rc ) warn_if_err(ERR_IO_STR("MPI_Comm_free failed"));
  }
#endif
}

tile MPI_handler::request_tile(void) {
  tile ret;
  ret = allocate_pages_tile(tile_size);
  assert(ret.data);
  ret.len = 0;
  return ret;
}

void MPI_handler::release_tile(tile t) {
  free_pages_tile(t);
}


void MPI_handler::add_group(MPI_pipe_group* group)
{
  if( DEBUG_MPI ) {
    printf("node %i adding group %p\n", iproc, group);
  }

  {
    pthread_held_mutex held_lock(&groups_lock);
    groups.push_back(group);

    // Reopen our syncronization queues.
    new_events.reopen();
  }
}
void MPI_handler::remove_group(MPI_pipe_group* group)
{
  size_t i,j;

  if( DEBUG_MPI ) {
    printf("node %i removing group %p\n", iproc, group);
  }

  {
    pthread_held_mutex held_lock(&groups_lock);
    for( i = 0, j = 0; i < groups.size(); i++ ) {
      if( groups[i] != group ) {
        groups[j++] = groups[i];
      }
    }
    groups.resize(j);

    if( DEBUG_MPI ) {
      printf("node %i after removing %i groups remain\n", iproc, (int) groups.size());
    }
  }

 
}

size_t MPI_handler::total_num_not_ended(void)
{
  size_t num = 0;
  for( size_t i = 0; i < groups.size(); i++ ) {
    num += groups[i]->total_num_not_ended();
  }
  return num;
}
 

size_t MPI_handler::total_num_sends_open(void)
{
  size_t num = 0;
  for( size_t i = 0; i < groups.size(); i++ ) {
    num += groups[i]->total_num_sends_open();
  }
  return num;
}

void MPI_handler::complete_recv_end(MPI_pipe_group* group, bin_num bin)
{
  group->bins[bin].num_not_ended--;
  if( group->bins[bin].num_not_ended == 0 ) {
    group->bins[bin].recv_pipe->full_tiles.close();
  }
}

void MPI_handler::complete_send_end(MPI_pipe_group* group, bin_num bin)
{
  group->bins[bin].send_pipe_is_open = false;
}

size_t MPI_handler::count_pending_type_mask(int type)
{
  size_t count = 0;
  for( size_t i = 0; i < pending.size(); i++ ) {
    if( pending[i].type & type ) count++;
  }
  return count;
}

void make_sure_signal_closed(void* state)
{
  MPI_handler* handler = (MPI_handler*) state;
  assert(handler);
  {
    pthread_held_mutex held_lock(&handler->signal_lock);

    handler->signal_state = SIGNAL_CLOSED;
  }
}

#ifdef HAVE_MPI_H
static int waitnew_query( void *extra_state, MPI_Status *status )
{
  /* Note that this query function is called only after
   * MPI_Grequest_complete
   */

  /* Set a default status */
  status->MPI_SOURCE = MPI_UNDEFINED;
  status->MPI_TAG = MPI_UNDEFINED;
  MPI_Status_set_cancelled( status, 0 );
  MPI_Status_set_elements( status, MPI_BYTE, 0 );

  make_sure_signal_closed(extra_state);

  return 0;
}

int waitnew_free( void *extra_state )
{
  /* The value returned by the free function is the error code returned by the wait/test function */

  make_sure_signal_closed(extra_state);

  return 0;
}
int waitnew_cancel( void *extra_state, int complete )
{
  make_sure_signal_closed(extra_state);

  return 0;
}
#endif

std::string MPI_handler::pending_string()
{
  std::vector<char> buf;
  buf.resize(1 + 100*pending.size());
  buf[0] = 0;

  int j=0;

  for( size_t i = 0; i < pending.size(); i++ ) {
    char type;
    switch (pending[i].type) {
      case TYPE_RECV_SIGNAL:
        type = 't';
        break;
      case TYPE_RECV:
        type = 'r';
        break;
      case TYPE_SEND:
        type = 's';
        break;
      case TYPE_SEND_END:
        type = 'e';
        break;
      case (TYPE_COMPLETE|TYPE_RECV_SIGNAL):
        type = 'T';
        break;
      case (TYPE_COMPLETE|TYPE_RECV):
        type = 'R';
        break;
      case (TYPE_COMPLETE|TYPE_SEND):
        type = 'S';
        break;
      case (TYPE_COMPLETE|TYPE_SEND_END):
        type = 'E';
        break;
      default:
        assert(0);
    }
    j += sprintf(&buf[j], "%c/%i/%p ",
                 type,
                 pending[i].recipient,
#ifdef HAVE_MPI_H
                 (void*) pending[i].request
#else
                 (void*) NULL
#endif
                 );
  }

  std::string ret(&buf[0]);

  return ret;
}

void MPI_handler::work(void)
{
#ifdef HAVE_MPI_H
  int rc;
#endif

  // Lock the groups structure...
  pthread_held_mutex held_lock(&groups_lock);

  // There are 2 things we need to do here.
  // 1) handle newly enqueued events
  // 2) complete MPI events that are done.
  
  if( DEBUG_MPI ) printf("node %i in work()\n", iproc);

  // Schedule a receive for each group.
  for( size_t i = 0; i < groups.size(); i++ ) {
    // Schedule a new recieve
    if( groups[i]->total_num_not_ended() > 0 ) {
      if( DEBUG_MPI ) printf("node %i scheduling recv for group %p\n", iproc, groups[i]);
      pending_t new_recv;
      new_recv.t = request_tile();
      if( DEBUG_MPI ) {
        printf("group %p adding recv tile %p\n", 
               groups[i], new_recv.t.data);
      }
      new_recv.bin = -1;
      new_recv.type = TYPE_RECV;
#ifdef HAVE_MPI_H
      new_recv.recipient = MPI_ANY_SOURCE;
#else
      new_recv.recipient = -1;
#endif
      new_recv.group = groups[i];
      new_events.push(new_recv);
    } else {
      if( DEBUG_MPI ) printf("node %i NOT scheduling recv for group %p total_num_not_ended()=0\n", iproc, groups[i]);
    }
  }
 
  while( 1 ) {

    bool canblock = true;
    if( ! USE_GREQUEST ) canblock = false;

    // Do we need to exit the loop?
    {
      size_t new_ones = new_events.size();
      size_t pending_ones = count_pending_type_mask(TYPE_SEND|TYPE_SEND_END);
      size_t total_not_ended = total_num_not_ended();
      size_t total_sends = total_num_sends_open();

      if( DEBUG_MPI && USE_GREQUEST>0 ) {
        printf("node %i in work() loop new=%li pending=%li not_ended=%li sends=%li\n",
               iproc, (long int) new_ones, (long int) pending_ones,
               (long int) total_not_ended, (long int) total_sends
               );
      }

      if( new_ones == 0 &&
          pending_ones == 0 &&
          total_not_ended == 0 &&
          total_sends == 0 ) {
        // We can quit this big event loop!
        break;
      }
    }


    // Add a generalized request to check for
    // a completed event, if there isn't already
    // one in pending.
    if( USE_GREQUEST ) {
      pthread_held_mutex held_lock(&signal_lock);

      if( (signal_state != SIGNAL_AVAILABLE) &&
          (signal_state != SIGNAL_SIGNALLED) ) {

        assert( signal_state == SIGNAL_WAITRETURNED ||
                signal_state == SIGNAL_NOTINITED );


        signal_state = SIGNAL_AVAILABLE;

#ifdef HAVE_MPI_H
        pending_t new_event;

        new_event.type = TYPE_RECV_SIGNAL;

        signal_request = MPI_REQUEST_NULL;
        rc = MPI_Grequest_start(waitnew_query, waitnew_free, waitnew_cancel,
                                this, &signal_request);
        if( rc ) throw error(ERR_IO_STR("MPI_Grequest_start failed"));

        new_event.request = signal_request;

        if( DEBUG_MPI ) {
          printf("node %i started signal request=%p\n", 
                 iproc, (void*) new_event.request);
        }
        pending.push_back(new_event);
#endif
      }
    }

    // Then, check to see if there are
    // any newly enqueued events.
    {
      pending_t new_event;
      while( new_events.pop_nowait(&new_event) ) {

#ifdef HAVE_MPI_H
        assert( new_event.request == MPI_REQUEST_NULL );
        //new_event.request = MPI_REQUEST_NULL;
        memset(&new_event.status, 0, sizeof(MPI_Status));
#endif
        
        switch (new_event.type) {
          case TYPE_RECV_SIGNAL:
            assert(0);
            break;

          case TYPE_RECV:
            // Start a recv.
            assert( new_event.bin == -1 );
            assert( new_event.group != NULL );

#ifdef HAVE_MPI_H
            assert( new_event.recipient == MPI_ANY_SOURCE );

             //printf("Irecv %p %zi\n", new_event.t.data, new_event.t.max);

            rc = MPI_Irecv(new_event.t.data,
                           new_event.t.max,
                           MPI_BYTE,
                           MPI_ANY_SOURCE,
                           1,
                           new_event.group->comm,
                           &new_event.request);
            if( rc ) {
              throw error(ERR_IO_STR("MPI_Irecv failed"));
            }
#endif

            if( DEBUG_MPI ) {
              printf("node %i:%s started receive for group %p with t.data=%p request=%p\n",
                     new_event.group->iproc, new_event.group->type.c_str(), new_event.group, new_event.t.data, 
#ifdef HAVE_MPI_H
                     (void*) new_event.request
#else
                     (void*) NULL
#endif
                     );
            }
            pending.push_back(new_event);
            break;

          case TYPE_SEND:
            assert( new_event.group );
            assert( new_event.bin >= 0 );
            assert( new_event.t.data );
            assert( new_event.t.len > 0 );

            if( new_event.group->bins[new_event.bin].recv_pipe ) {

              if( DEBUG_MPI ) {
                printf("node %i:%s locally resolved send bin %i len=%i t.data=%p\n", 
                       new_event.group->iproc, new_event.group->type.c_str(), (int) new_event.bin,
                       (int) new_event.t.len, new_event.t.data);
              }
 
 
              // The recv' pipe is local
              // Do a short-circuit.
              new_event.group->bins[new_event.bin].recv_pipe->full_tiles.push(new_event.t);
              // Add a new tile to the sender buffer, since the
              // receive pipe will free that one.
              {
                tile t = request_tile();
                if( DEBUG_MPI ) {
                  printf("group %p adding tile (local-send) %p\n", new_event.group, t.data);
                }
                new_event.group->empty_send_tiles.push(t);
              }
            } else {
              // Set the bin number.
              assert( new_event.t.len > 0 ); // 0 means EOF
              assert( new_event.t.len + sizeof(bin_num) <= tile_size );
              memcpy( PTR_ADD(new_event.t.data, new_event.t.len),
                      &new_event.bin,
                      sizeof(bin_num) );
              new_event.t.len += sizeof(bin_num);

#ifdef HAVE_MPI_H
            
              //if( new_event.t.len > 530400 ) printf("Isend %p %zi %zi\n", new_event.t.data, new_event.t.len, new_event.t.max);

              // Start an async send.
              rc = MPI_Isend(new_event.t.data,
                             new_event.t.len,
                             MPI_BYTE,
                             new_event.recipient,
                             1,
                             new_event.group->comm,
                             &new_event.request );
              if( rc ) throw error(ERR_IO_STR("MPI_Isend failed"));
#else
              assert(0);
#endif

              if( DEBUG_MPI ) {
                printf("node %i:%s started send bin %i with t.data=%p request=%p to %i\n", 
                       new_event.group->iproc, new_event.group->type.c_str(), (int) new_event.bin, new_event.t.data,
#ifdef HAVE_MPI_H
                       (void*) new_event.request,
#else
                       (void*) NULL,
#endif
                       new_event.recipient);
              }
              pending.push_back(new_event);
            }
            break;

          case TYPE_SEND_END:
            assert( new_event.group );
            assert( new_event.bin >= 0 );
            assert( ! new_event.t.data );
            assert( new_event.t.len == 0 );

            if( new_event.group->bins[new_event.bin].recv_pipe ) {
              // The recv' pipe is local
              // Do a short-circuit.
              if( DEBUG_MPI ) {
                printf("node %i:%s locally resolved send end bin %i\n", 
                       new_event.group->iproc, new_event.group->type.c_str(), (int) new_event.bin);
              }

              complete_recv_end(new_event.group, new_event.bin);
              complete_send_end(new_event.group, new_event.bin);
 
              // Don't block since we maybe need to exit the loop.
              canblock = false;
            } else {

#ifdef HAVE_MPI_H
              // Start an async send.
              rc = MPI_Isend(&new_event.group->bins[new_event.bin].b,
                             sizeof(bin_num),
                             MPI_BYTE,
                             new_event.recipient,
                             1,
                             new_event.group->comm,
                             &new_event.request );
              if( rc ) throw error(ERR_IO_STR("MPI_Isend end failed"));
#else
              assert(0);
#endif

              if( DEBUG_MPI ) {
                printf("node %i:%s started send end bin %i with len=%i request=%p to %i\n", 
                       new_event.group->iproc, new_event.group->type.c_str(), (int) new_event.bin, (int) sizeof(bin_num),
#ifdef HAVE_MPI_H
                       (void*) new_event.request,
#else
                       (void*) NULL,
#endif
                       new_event.recipient);
              }
              pending.push_back(new_event);
            }
            break;

          default:
            // Unknown message!
            assert(0);
        }


      } // end while new events.
    }

    // OK, now check for completed MPI events.
#ifdef HAVE_MPI_H
    int got_count;
    std::vector<MPI_Request> requests;
    std::vector<int> indices;
    std::vector<MPI_Status> statuses;
#endif
    if( pending.size() > 0 ) {
#ifdef HAVE_MPI_H
      requests.resize(pending.size());
      indices.resize(pending.size());
      statuses.resize(pending.size());

      got_count = 0;
      for( size_t i = 0; i < pending.size(); i++ ) {
        requests[i] = pending[i].request;
      }
      for( size_t i = 0; i < indices.size(); i++ ) {
        indices[i] = -1;
      }

      memset(&statuses[0], 0, sizeof(MPI_Status)*statuses.size());

      if( DEBUG_MPI && USE_GREQUEST>0 ) {
        std::string buf = pending_string();
        printf("node %i about to test/wait: %s\n", iproc, buf.c_str());
      }

      if( canblock ) {
        rc = MPI_Waitsome(pending.size(),
                          &requests[0],
                          &got_count,
                          &indices[0],
                          &statuses[0]);
        if( rc ) throw error(ERR_IO_STR("MPI_Waitsome failed"));
      } else {
        rc = MPI_Testsome(pending.size(),
                          &requests[0],
                          &got_count,
                          &indices[0],
                          &statuses[0]);
        if( rc ) throw error(ERR_IO_STR("MPI_Testsome failed"));
      }

      // Report any that we completed.
      for( int i = 0; i < got_count; i++ ) {
        int got_index = indices[i];
        pending_t* p = &pending[got_index];
        int count;

        if( DEBUG_MPI ) {
          printf("node %i completed request %p (%i)\n", iproc, (void*) p->request, got_index );
        }

        // Set the status in the pending object.
        p->status = statuses[i];

        // Set the request in the pending object.
        p->request = requests[got_index];

        // MPI_Wait should have cleared out the request.
        assert(p->request == MPI_REQUEST_NULL);

        // Set the completion flag in the type.
        p->type |= TYPE_COMPLETE;

        if( p->status.MPI_ERROR ) {
          fprintf(stderr, "Got MPI error %i\n", p->status.MPI_ERROR);
          throw error(ERR_IO_STR("MPI command eventually returned error"));
        }

        rc = MPI_Get_count(&p->status, MPI_BYTE, &count);
        if( rc ) throw error(ERR_IO_STR("MPI_Get_count failed"));

        p->t.len = count;


        switch (pending[got_index].type) {
          case (TYPE_COMPLETE|TYPE_RECV_SIGNAL):
            // OK, great!
            if( DEBUG_MPI ) {
              printf("node %i received signal\n", iproc);
            }
            {
              pthread_held_mutex held_lock(&signal_lock);
              assert( signal_state == SIGNAL_CLOSED );
              signal_state = SIGNAL_WAITRETURNED;
              signal_request = MPI_REQUEST_NULL;
            }
            break;
          case (TYPE_COMPLETE|TYPE_RECV):
            // Send received data to the appropriate recv pipe,
            // or handle it correctly if it's an "END" message.
            {
              bin_num bin = -1;
              assert( p->t.len >= sizeof(bin_num) );
              // Get bin # from end of tile
              // note - bin # cannot be stored in MPI tag
              // since we would do recv. any tag.
              p->t.len -= sizeof(bin_num);
              memcpy( &bin,
                      PTR_ADD(p->t.data, p->t.len),
                      sizeof(bin_num) );

              assert( bin >= 0 );
              assert( bin < p->group->n_bins );

              assert( p->group->bins[bin].recv_pipe );

              if( p->t.len > 0 ) {
                if( DEBUG_MPI ) {
                  printf("node %i:%s received %li-bytes of data on bin %i with t.data=%p\n", 
                         p->group->iproc, p->group->type.c_str(), (long int) p->t.len, (int) bin, p->t.data);
                }
                // Send the tile to the receive pipe.
                p->group->bins[bin].recv_pipe->full_tiles.push(p->t);
                p->t = empty_tile();
              } else {
                if( DEBUG_MPI ) {
                  printf("node %i:%s received EOF on bin %i\n", 
                         p->group->iproc, p->group->type.c_str(), (int) bin);
                }
                // It's the EOF message!
                complete_recv_end(p->group, bin);
                release_tile(p->t);
                p->t = empty_tile();
              }
            }

            // Schedule a new recieve
            if( p->group->total_num_not_ended() > 0 ) {
              pending_t new_recv;
              new_recv.t = request_tile();
              if( DEBUG_MPI ) {
                printf("group %p adding recv tile %p\n", 
                       p->group, new_recv.t.data);
              }
              new_recv.bin = -1;
              new_recv.type = TYPE_RECV;
              new_recv.recipient = MPI_ANY_SOURCE;
              new_recv.group = p->group;
              new_events.push(new_recv);
            }
            break;
          case (TYPE_COMPLETE|TYPE_SEND):
            if( DEBUG_MPI ) {
              printf("node %i:%s completed send to bin %i\n", 
                     p->group->iproc, p->group->type.c_str(), (int) p->bin);
            }
            p->group->empty_send_tiles.push(p->t);
            p->t = empty_tile();
            break;
          case (TYPE_COMPLETE|TYPE_SEND_END):
            // Great, completed sending an EOF.
            complete_send_end(p->group, p->bin);
            if( DEBUG_MPI ) {
              printf("node %i:%s completed send EOF to bin %i\n", 
                     p->group->iproc, p->group->type.c_str(), (int) p->bin);
            }
            break;
          default:
            assert(0); // unknown messages type!
        } // end switch

      } // for completed requests
#else

      // Just wait on the signal condition variable.
      if( canblock ) {
        pthread_held_mutex lock(&signal_lock);

        if( signal_state == SIGNAL_AVAILABLE ) {
          signal_cond.wait(&signal_lock);
        }

        assert( signal_state == SIGNAL_SIGNALLED );
        signal_state = SIGNAL_WAITRETURNED;
      }

#endif

      if( DEBUG_MPI && USE_GREQUEST>0 ) {
        std::string buf = pending_string();
        printf("node %i after test/wait: %s\n", iproc, buf.c_str());
      }

      // Remove any completed things from pending.
      {
        size_t i,j;
        for( i = 0, j = 0; i < pending.size(); i++ ) {
          if( (pending[i].type & TYPE_COMPLETE) == 0 ) {
            // Not yet complete.
            pending[j++] = pending[i];
          }
        }
        pending.resize(j);
      }

      if( DEBUG_MPI && USE_GREQUEST>0 ) {
        std::string buf = pending_string();
        printf("node %i after cleanup: %s\n", iproc, buf.c_str());
      }

    } // pending.size() > 0

  } // while loop

  if( DEBUG_MPI ) {
    printf("node %i in finishing work()\n", iproc);
  }

  new_events.close();

  // Make the signal no longer available.
  signal();

  // Complete any requests in pending if they remain.
  // There should only be generalized requests,
  // or RECVs that we need to cancel..
  for( size_t i = 0; i < pending.size(); i++ ) {
    assert( pending[i].type == TYPE_RECV_SIGNAL ||
            pending[i].type == TYPE_RECV );
#ifdef HAVE_MPI_H
    if( pending[i].type == TYPE_RECV ) {
      // We need to cancel it!
      rc = MPI_Cancel( &pending[i].request );
      if( rc ) throw error(ERR_IO_STR("MPI_Cancel failed"));
    }
    // Wait for it so any resources will be freed.
    rc = MPI_Wait( & pending[i].request, MPI_STATUS_IGNORE );
    if( rc ) throw error(ERR_IO_STR("MPI_Wait failed"));
#endif
    if( pending[i].type == TYPE_RECV ) {
      // We also need to free the tile associated with the request.
      release_tile(pending[i].t);
    }
  }

  pending.clear();


  {
    pthread_held_mutex held_lock(&signal_lock);
    signal_state = SIGNAL_NOTINITED;
#ifdef HAVE_MPI_H
    signal_request = MPI_REQUEST_NULL;
#endif
  }
}

void MPI_handler::signal()
{
#ifdef HAVE_MPI_H
  int rc;
#endif

  if( USE_GREQUEST ) {
    pthread_held_mutex held_lock(&signal_lock);

    if( signal_state == SIGNAL_AVAILABLE ) {
      // Cause the MPI_Wait to stop.
      signal_state = SIGNAL_SIGNALLED;
#ifdef HAVE_MPI_H
      assert( signal_request != MPI_REQUEST_NULL);
      rc = MPI_Grequest_complete( signal_request );
      if( rc ) throw error(ERR_IO_STR("MPI_Grequest_complete failed"));
#else
      signal_cond.broadcast();
#endif
      if( DEBUG_MPI ) {
        printf("node %i signalled request=%p\n", iproc,
#ifdef HAVE_MPI_H
            (void*) signal_request
#else
            (void*) NULL
#endif
            );
      }
      //signal_request = MPI_REQUEST_NULL; // it's only in pending now.
    }
  }
}
  
void MPI_handler::enqueue_event(pending_t p)
{
  // First, put the event in new_events.
  new_events.push(p);
  
  // Then, send the worker thread an MPI
  // message to cause it to wake up & add this
  // work.
  signal();
}

MPI_pipe_group::MPI_pipe_group(MPI_handler* handler, std::string type, std::vector<int> bin_owners)
  : handler(handler), comm(NULL_COMM), type(type),
    n_bins(bin_owners.size()),
    bins(),
    empty_send_tiles()
{
#ifdef HAVE_MPI_H
  int rc;

  rc = MPI_Comm_dup(handler->comm, &comm);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_dup failed"));
#endif

  // Get the rank and number of processes in this communicator.
  nproc = MPI_handler::get_nproc(comm);
  iproc = MPI_handler::get_iproc(comm);

  bins.reserve(n_bins);

  // Add the pipes
  for( bin_num b = 0; b < n_bins; b++ ) {
    assert( 0 <= bin_owners[b] );
    assert( bin_owners[b] < nproc );
    bin_info x;
    x.b = b;
    x.mpi_owner = bin_owners[b];
    x.send_pipe = NULL;
    x.recv_pipe = NULL;
    x.num_not_ended = 0;
    x.send_pipe_is_open = false;

    if( bin_owners[b] == iproc ) {
      x.recv_pipe = new MPI_recv_pipe(this, b);
      x.num_not_ended = nproc; // even expect end from ourselves!
    }

    x.send_pipe = new MPI_send_pipe(this, x.mpi_owner, b);
    x.send_pipe_is_open = true;

    bins.push_back(x);
  }

  // Add the tiles.
  size_t add_tiles = n_bins * handler->num_tiles_per_send_pipe;
  if( DEBUG_MPI ) {
    printf("group requesting %li bytes\n", 
         add_tiles * handler->tile_size );
  }
  for( size_t i = 0; i < add_tiles; i++ ) {
    tile t = handler->request_tile();
    if( DEBUG_MPI ) {
      printf("group %p adding tile %p\n", this, t.data);
    }
    empty_send_tiles.push(t);
  }
}

MPI_pipe_group::~MPI_pipe_group()
{

  empty_send_tiles.close();
  while( 1 ) {
    tile t = empty_send_tiles.pop();
    if( ! t.data ) break;
    if( DEBUG_MPI ) {
      printf("group %p releasing tile %p\n", this, t.data);
    }
    handler->release_tile(t);
  }

  for( size_t b = 0; b < bins.size(); b++ ) {
    if( bins[b].send_pipe ) delete bins[b].send_pipe;
    if( bins[b].recv_pipe ) delete bins[b].recv_pipe;
  }

#ifdef HAVE_MPI_H
  // Free the communicator we allocated.
  if( comm != MPI_COMM_NULL ) {
    int rc = MPI_Comm_free(&comm);
    if( rc ) warn_if_err(ERR_IO_STR("MPI_Comm_free failed"));
  }
#endif

}

void MPI_pipe_group::barrier(void)
{
#ifdef HAVE_MPI_H
  int rc;
  if( nproc > 1 ) {
    rc = MPI_Barrier(comm);
    if( rc ) throw error(ERR_IO_STR("MPI_Barrier failed"));
  }
#endif
}

size_t MPI_pipe_group::total_num_not_ended(void)
{
  size_t total=0;
  for(size_t i = 0; i < bins.size(); i++ ) {
    total += bins[i].num_not_ended;
  }
  return total;
}

size_t MPI_pipe_group::total_num_sends_open(void)
{
  size_t total=0;
  for(size_t i = 0; i < bins.size(); i++ ) {
    if( bins[i].send_pipe_is_open ) total++;
  }
  return total;
}

bool MPI_pipe_group::is_local(bin_num b)
{
  return bins[b].mpi_owner == iproc;
}

int MPI_pipe_group::owner(bin_num b)
{
  return bins[b].mpi_owner;
}

MPI_send_pipe::MPI_send_pipe(MPI_pipe_group* group, int recipient, bin_num bin)
  : group(group), recipient(recipient), bin(bin), closed(false)
{
}

MPI_send_pipe::~MPI_send_pipe()
{
  assert(closed);
}

tile MPI_send_pipe::get_empty_tile()
{
  tile t = group->empty_send_tiles.pop();
  if( DEBUG_MPI ) {
    printf("group %p send_pipe %p got tile %p\n", group, this, t.data);
  }
  t.len = 0;
  // Leave room to store the bin number at the end.
  t.max = group->handler->tile_size - sizeof(bin_num);
  return t;
}

void MPI_send_pipe::put_full_tile(const tile& t)
{
  if( DEBUG_MPI ) {
    printf("group %p send_pipe %p enqueuing tile %p\n", group, this, t.data);
  }

  if( t.len == 0 ) {
    group->empty_send_tiles.push(t);
    return;
  }

  pending_t p;
  p.t = t;
  p.bin = bin;
  p.type = TYPE_SEND;
  p.recipient = recipient;
  p.group = group;

  group->handler->enqueue_event(p);
}

void MPI_send_pipe::close_full()
{
  if( DEBUG_MPI ) {
    printf("node %i send_pipe %p in close_full()\n", 
           group->iproc, this);
  }

  pending_t p;
  p.t = empty_tile();
  p.bin = bin;
  p.type = TYPE_SEND_END;
  p.recipient = recipient;
  p.group = group;

  group->handler->enqueue_event(p);

  assert(!closed);
  closed = true;
}

size_t MPI_send_pipe::get_tile_size()
{
  return group->handler->tile_size;
}

size_t MPI_send_pipe::get_num_tiles()
{
  return group->handler->num_tiles_per_send_pipe;
}

bool MPI_send_pipe::is_thread_safe()
{
  return true;
}

MPI_recv_pipe::MPI_recv_pipe(MPI_pipe_group* group, bin_num bin)
  : group(group), bin(bin), full_tiles(), closed(false)
{
}

MPI_recv_pipe:: ~MPI_recv_pipe()
{
  assert(closed);
}


const tile MPI_recv_pipe::get_full_tile()
{
  tile t = full_tiles.pop();
  if( DEBUG_MPI ) {
    printf("group %p recv_pipe %p got full tile %p\n", group, this, t.data);
  }
  return t;
}

void MPI_recv_pipe::put_empty_tile(const tile& t)
{
  if( DEBUG_MPI ) {
    printf("group %p recv_pipe %p releasing tile %p\n", group, this, t.data);
  }
  group->handler->release_tile(t);
}

void MPI_recv_pipe::close_empty()
{
  if( DEBUG_MPI ) {
    printf("node %i recv_pipe %p in close_empty()\n", 
           group->iproc, this);
  }
  assert(!closed);
  closed = true;
}

void MPI_recv_pipe::wait_full()
{
  return full_tiles.wait();
}

size_t MPI_recv_pipe::get_tile_size()
{
  return group->handler->tile_size;
}

size_t MPI_recv_pipe::get_num_tiles()
{
  return INT_MAX;
}

bool MPI_recv_pipe::is_thread_safe()
{
  return true;
}

