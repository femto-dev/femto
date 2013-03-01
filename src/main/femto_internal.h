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

  femto/src/main/femto_internal.h
*/
#ifndef _FEMTO_INTERNAL_H_
#define _FEMTO_INTERNAL_H_

#include "femto.h"
#include "error.h"
#include "server.h"

#include <time.h>


void femto_cleanup_request(femto_request_t* req);

error_t femto_start_server_err(femto_server_t* srv, int verbose);

// Starts the passed query and waits for it to return.
// Does not free or cleanup the passed query.
error_t femto_run_query(femto_server_t* srv, query_entry_t* query);

error_t femto_setup_request_err(femto_request_t* req, 
                                query_entry_t* query,
                                int free_query);


error_t femto_begin_request_err(femto_server_t* srv, femto_request_t* req);
error_t femto_wait_request_err(femto_server_t* srv, femto_request_t* req);
error_t femto_timedwait_request_err(femto_server_t* srv, femto_request_t* req, const struct timespec* ts, int* completed);

error_t femto_loc_for_path_err(femto_server_t* srv, const char* index_path, index_locator_t* loc);

error_t femto_setup_generic_request_err(femto_request_t* req,
                                        // IN
                                        femto_server_t* srv,
                                        const char* index_path,
                                        const char* request);

error_t femto_response_for_generic_request_err(femto_request_t* req,
                                               femto_server_t* srv,
                                               char** response);


error_t parallel_count(femto_server_t* srv, index_locator_t loc, int npats, int* plen, alpha_t** pats, int64_t* first, int64_t* last);
error_t parallel_locate(femto_server_t* srv, index_locator_t loc,
                        int npats, int* plen, alpha_t** pats,
                        int max_occs_each,
                        int* noccs, int64_t** offsets);
error_t serial_locate(femto_server_t* srv, index_locator_t loc,
                        int npats, int* plen, alpha_t** pats,
                        int max_occs_each,
                        int* noccs, int64_t** offsets);
error_t parallel_locate_range(femto_server_t* srv, index_locator_t loc,
                        int64_t first, int64_t last,
                        int64_t* offsets);


#endif

