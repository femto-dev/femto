/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/handle_request.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "femto.h"

int main(int argc, char** argv)
{
  int rc;
  femto_server_t srv;
  femto_request_t* req = NULL;
  char* index_path = argv[1];
  char* request = argv[2];
  char* response = NULL;

  if( argc != 3 ) {
    printf("Usage: %s <index> <request>\n", argv[0]);
    return -1;
  }

  rc = femto_start_server(&srv);
  assert(!rc);

  printf("Index:%s\n", index_path);
  printf("Request:\n%s\n", request);

  rc = femto_create_generic_request(&req, &srv, index_path, request);
  assert(!rc);

  rc = femto_begin_request(&srv, req);
  assert(!rc);

  rc = femto_wait_request(&srv, req);
  assert(!rc);

  rc = femto_response_for_generic_request(req, &srv, &response);
  assert(!rc);

  printf("Response:\n%s\n", response);

  free(response);
  femto_destroy_request(req);

  femto_stop_server(&srv);

  return 0;
}
