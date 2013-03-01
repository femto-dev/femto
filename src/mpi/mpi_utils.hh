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

  femto/src/mpi/mpi_utils.hh
*/
#ifndef __MPI_UTILS_HH__
#define __MPI_UTILS_HH__

extern "C" {
#include "config.h"
}

#ifdef HAVE_MPI_H
#include <mpi.h>
#endif

#include <cstring>

#include "pipelining.hh"

#ifdef HAVE_MPI_H
typedef MPI_Comm My_comm;
#define DEFAULT_COMM MPI_COMM_WORLD
#define NULL_COMM MPI_COMM_NULL
#define NULL_REQUEST MPI_REQUEST_NULL
#else
typedef void* My_comm;
#define DEFAULT_COMM NULL
#define NULL_COMM NULL
#define NULL_REQUEST NULL
#endif

// Note -- all nodes must call mpi_copy_file at the same time.
// This routine will try to detect a network share; in that case it
// will skip the copy. This detection works as follows:
// 1) Are the paths the same?
// 2) Is the device a network filesystem?
// 3) Is there a file of the same length on both sender and reciever?
// If so, we don't copy.
void mpi_copy_file(My_comm in_comm, const std::string & from_fname, int from_proc, const std::string & to_fname, int to_proc, bool move_not_copy=false);
void mpi_move_file(My_comm in_comm, const std::string & from_fname, int from_proc, const std::string & to_fname, int to_proc);

void mpi_barrier(My_comm in_comm);

template<typename Record>
void mpi_share_vector(My_comm in_comm, std::vector<Record>* vec, int src_proc )
{
  assert(vec);

#ifdef HAVE_MPI_H
  int nproc=1;
  int iproc=0;
  int rc;

  // Set the rank and number of processes in this communicator.
  rc = MPI_Comm_size(in_comm,&nproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_size failed"));
  rc = MPI_Comm_rank(in_comm,&iproc);
  if( rc ) throw error(ERR_IO_STR("MPI_Comm_rank failed"));

  if(nproc > 1) {
    // First send number of elements.
    unsigned long long n_elems = 0;
    if( iproc == src_proc ) {
      n_elems = vec->size();
    }

    rc = MPI_Bcast(&n_elems, 1, MPI_LONG_LONG_INT, 0, in_comm);
    if( rc ) throw error(ERR_IO_STR("MPI_Bcast failed"));

    // Resize vector on all nodes non-source nodes.
    if( iproc != src_proc ) vec->resize(n_elems);

    // Broadcast vector data.
    rc = MPI_Bcast(& (*vec)[0], n_elems*sizeof(Record), MPI_BYTE, 0, in_comm);
    if( rc ) throw error(ERR_IO_STR("MPI_Bcast failed"));
  }
#endif
}

enum { TYPE_NONE=0,
       TYPE_RECV_SIGNAL=1,
       TYPE_RECV=2,
       TYPE_SEND=4,
       TYPE_SEND_END=8,
       TYPE_COMPLETE=16 };

enum { SIGNAL_NOTINITED=0,
       SIGNAL_AVAILABLE=1,
       SIGNAL_SIGNALLED=2,
       SIGNAL_CLOSED=3,
       SIGNAL_WAITRETURNED=4,
     };


typedef int bin_num;

// forward declare...
struct MPI_pipe_group;

struct pending_t {
  // These must be filled out.
  tile t;
  bin_num bin; // -1 for a recv.
  int type; // TYPE_RECV or TYPE_SEND
  int recipient; // ANY for recvs.
  MPI_pipe_group* group;
#ifdef HAVE_MPI_H
  // Request is filled out when this object is queued.
  MPI_Request request;
  // Status is filled out when it's completed.
  MPI_Status status;
#endif
  pending_t()
    : t(), bin(-1), type(TYPE_NONE), recipient(-1), group(NULL)
#ifdef HAVE_MPI_H
      , request(MPI_REQUEST_NULL)
#endif
  {
#ifdef HAVE_MPI_H
    memset(&status, 0, sizeof(MPI_Status));
#endif
  }
};

/* Since we want to handle all MPI communications together,
 * in a single event loop, we have an MPI_pipes object.
 * This object handles sub-pipes (MPI_pipe).
 *
 * The MPI handler is partially thread-safe..
 */
struct MPI_handler : private uncopyable {
 public:
  // These parts are set only in constructor.
  size_t tile_size; // MPI message size. must fit into an int.
  size_t num_tiles_per_send_pipe; // rcv's are always allocated.
  My_comm comm;
  int iproc;
  int nproc;
 private:
  // These ones are not thread-safe.
  // Maximum number of sends before we wait.
  std::vector< pending_t > pending;


  size_t count_pending_type_mask(int type);

 
 public:
  // The following methods must only be invoked by the main thread.
  static void init_MPI(int *argc, char*** argv);
  static void finalize_MPI(void);
  static int get_nproc(My_comm comm = DEFAULT_COMM);
  static int get_iproc(My_comm comm = DEFAULT_COMM);

  MPI_handler(size_t tile_size, size_t num_tiles_per_pipe, My_comm in_comm);
  ~MPI_handler();

 private:
  pthread_mutex groups_lock;
  std::vector< MPI_pipe_group* > groups;
 public:
  // Add group adds another n_bins tiles to empty_send_tiles.
  void add_group(MPI_pipe_group* group);
  // Remove group takes away n_bins.
  void remove_group(MPI_pipe_group* group);

  size_t total_num_not_ended(void);
  size_t total_num_sends_open(void);

  std::string pending_string();

  void complete_recv_end(MPI_pipe_group* group, bin_num bin);
  void complete_send_end(MPI_pipe_group* group, bin_num bin);

  // Do the MPI work.. Stops when there are no
  // more pending events.
  void work(void);

  // These elemnts/methods can be invoked by any thread.
  
  // Tiles available for recvs.
  tile request_tile(void); // should have 2x #groups tiles
  void release_tile(tile t);
 
  task_queue<pending_t> new_events;

  // Stores a single active request.
  pthread_mutex signal_lock;
#ifdef HAVE_MPI_H
   MPI_Request signal_request;
#else
   pthread_cond signal_cond;
#endif
   int signal_state;

 private:
  void signal(void);

 public:
  void enqueue_event(pending_t p);

};

// Forward declare...
struct MPI_send_pipe;
struct MPI_recv_pipe;

struct MPI_pipe_group : private uncopyable {
  MPI_handler* handler;
  My_comm comm;
  std::string type;
  int iproc;
  int nproc;
  bin_num n_bins;

  struct bin_info {
    bin_num b; // used for sending bin end.
    int mpi_owner; // which MPI proc owns this bin?
    // send pipes... exist for all bins.
    MPI_send_pipe* send_pipe;
    // True if the send pipe is still open.
    bool send_pipe_is_open;
    // recv pipes... only exist at bins controlled by iproc.
    MPI_recv_pipe* recv_pipe;
    // When we have a recv pipe, we keep track of how many 
    // "EOF" messages we've got -- by decrementing this number.
    // Since we have a local send pipe, this number starts out as nproc.
    // We're done when it's 0.
    size_t num_not_ended;
  };

  std::vector<bin_info> bins;

  MPI_pipe_group(MPI_handler* handler, std::string type, std::vector<int> bin_owners);
  ~MPI_pipe_group();
  void barrier(void);
  size_t total_num_not_ended(void);
  size_t total_num_sends_open(void);

  bool is_local(bin_num b);
  int owner(bin_num b);

  // Unlike the others, this one can be used from any thread.
  tile_list empty_send_tiles; // should have AT LEAST #bins; preferably 2x.
};

// Send data from one node to another node.
struct MPI_send_pipe : public write_pipe, private uncopyable {
  MPI_pipe_group* group;
  int recipient;
  bin_num bin;
  bool closed;

  MPI_send_pipe(MPI_pipe_group* group, int recipient, bin_num bin);
  virtual ~MPI_send_pipe();
  virtual tile get_empty_tile();
  virtual void put_full_tile(const tile& t);
  virtual void close_full();

  virtual size_t get_tile_size();
  virtual size_t get_num_tiles();
  virtual bool is_thread_safe();
};

struct MPI_recv_pipe : public read_pipe, private uncopyable {
  MPI_pipe_group* group;
  bin_num bin;
  tile_list full_tiles;
  bool closed;

  MPI_recv_pipe(MPI_pipe_group* group, bin_num bin);
  virtual ~MPI_recv_pipe();
  virtual const tile get_full_tile();
  virtual void put_empty_tile(const tile& t);
  virtual void close_empty();
  virtual void wait_full();

  virtual size_t get_tile_size();
  virtual size_t get_num_tiles();
  virtual bool is_thread_safe();
};


#endif
