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

  femto/src/main/femto.h
*/
#ifndef _FEMTO_H_
#define _FEMTO_H_

#include <inttypes.h>
#include <time.h> // struct timespec

int femto_version(const char**version);

// forward declare..
struct shared_server_state;
struct femto_server {
  struct shared_server_state* state;
};
typedef struct femto_server femto_server_t;

struct femto_request;
typedef struct femto_request femto_request_t;

// all these functions return an error code, or 0 for no error.
int femto_start_server(/*OUT*/ femto_server_t* srv);
void femto_stop_server(/*IN*/ femto_server_t* srv);

/* All of the functions below have the same model:
 * they start some requests in the running server
 * and then call functions to wait (possibly with timeout)
 * for the completion of these requests.
 *
 * To use, look for a femto_setup_XXX_request. Call that
 * to initialize a request with the question you're asking.
 * Then call:
 * femto_begin_request
 * femto_wait_request
 * femto_get_result_XXX_request to get the result
 * femto_destroy_request
 */

int femto_begin_request(femto_server_t* srv, femto_request_t* req);

/* It is not necessary to call femto_wait_request or femto_get_result;
 * femto_cancel_request will block until the request is actually
 * cancelled.
 */
int femto_cancel_request(femto_server_t* srv, femto_request_t* req);

int femto_wait_request(femto_server_t* srv, femto_request_t* req);

// note ts is an absolute time (as in man pthread_cond_wait)
int femto_timedwait_request(femto_server_t* srv, femto_request_t* req, const struct timespec* ts, int* completed);

void femto_destroy_request(femto_request_t* req);

/*
 * For each request, we'll write the request starting with '< ' and
 * what is returned starting with '> '. These angle brackets are not
 * part of the request or response.
 *
 * Multiple requests could be supported at once -- simply include newlines
 * between the requests. Not yet supported.
 *
 * START_ROW/END_ROW are decimal numbers
 * CHARACTER is a numeric character value
 * OFFSETS? is 1 for offsets, 0 for just documents
 *
 * > find_strings pattern
 * < {"matches":[
 * <   {"range":[START_ROW,END_ROW], "cost":COST, "match":[CHAR,CHAR,...]},
 * <  ...,
 * <  ]
 * < }
 *
 * > find_docs MAX_MATCHES OFFSETS? pattern
 * > {"results":[
 * >   {"doc_info":INFO_STRING, "offsets":[NUM,NUM,...]},
 * <  ...,
 * <  ]
 * < }
 * -- note "offsets" are optional
 *
 * > docs_for_range MAX_MATCHES OFFSETS? START_ROW END_ROW
 * > {"range":[START_ROW,END_ROW],
 *    "results":[
 * >    {"doc_info":INFO_STRING, "offsets":[NUM,NUM,...]},
 * <  ...,
 * <  ]
 * < }
 * -- note "offsets" are optional
 *
 * > string_rows CHARACTER CHARACTER...
 * < {"range":[START_ROW,END_ROW]}
 *
 * > string_rows_all CHARACTER CHARACTER...
 * < {"left":[
 * <    {"ch":CHAR, "range":[START_ROW,END_ROW]},
 * <    ...,
 * <  ],
 * <  "right":[
 * <    {"ch":CHAR, "range":[START_ROW,END_ROW]},
 * <    ...,
 * <  ]
 * < }
 * -- where the "left" section is additions of characters to the left
 *      and the "right" section is additions of characters to the right.
 *
 * > string_rows_left CHARACTER CHARACTER...
 * < {"left":[
 * <   {"ch":CHAR, "range":[START_ROW,END_ROW]},
 * <   ...,
 * <  ]
 * < }
 *
 * > string_rows_right CHARACTER CHARACTER...
 * < {"right":[
 * <   {"ch":CHAR, "range":[START_ROW,END_ROW]},
 * <   ...,
 * <  ]
 * < }
 */
int femto_create_generic_request(femto_request_t** req,
                                // IN
                                femto_server_t* srv,
                                const char* index_path,
                                const char* request);

// Response must be freed by caller.
int femto_response_for_generic_request(femto_request_t* req,
                                       femto_server_t* srv,
                                       char** response);

#endif
