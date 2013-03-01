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

  femto/src/utils/file_find.c
*/
#include "file_find.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>

#include "util.h"

static
int cmpstringp(const void *p1, const void *p2)
{
   /* The actual arguments to this function are "pointers to
      pointers to char", but strcmp(3) arguments are "pointers
      to char", hence the following cast plus dereference */

   return strcmp(* (char * const *) p1, * (char * const *) p2);
}

static
error_t set_curpath(file_find_state_t* s)
{
  int total_len = 0;

  // Set the cur_path based on what's in names_stack.
  for( int d = 0; d <= s->names_depth; d++ ) {
    char* str = s->names_stack[d][s->names_i[d]];

    total_len += strlen(str);
    total_len++; // room for '/'
  }
  total_len++; // room for '\0'

  if( total_len > s->path_max ) {
    s->path_max += 128 + total_len;
    s->cur_path = (char*) realloc(s->cur_path, s->path_max );
    if( ! s->cur_path ) return ERR_MEM;
  }

  // create cur_path from the names.
  total_len = 0;
  for( int d = 0; d <= s->names_depth; d++ ) {
    char* str = s->names_stack[d][s->names_i[d]];

    if( d != 0 ) s->cur_path[total_len++] = '/';
    strcpy(&s->cur_path[total_len], str);
    total_len += strlen(str);
  }
  // Add the NULL.
  s->cur_path[total_len++] = '\0';

  //printf("cur_path is now %s\n", s->cur_path);

  return ERR_NOERR;
}

void print_stack(file_find_state_t* s)
{
  printf("names stack is:\n");
  for( int d = 0; d <= s->names_depth; d++ ) {
    printf("d=%i ", d);
    for( int i = 0; s->names_stack[d][i]; i++ ) {
      if( i == s->names_i[d] ) {
        printf("*%s* ", s->names_stack[d][i]);
      } else {
        printf("%s ", s->names_stack[d][i]);
      }
    }
    printf("\n");
  }
  printf("curpath is %s\n", s->cur_path);
}

static
error_t go_up(file_find_state_t* s)
{
  while( s->names_depth > 0 &&
         s->names_stack[s->names_depth][s->names_i[s->names_depth]] == NULL ) {
    // Remove this entry from the stack.
    for( int i = 0; i < s->names_i[s->names_depth]; i++ ) {
      char* str = s->names_stack[s->names_depth][i];
      free(str);
      s->names_stack[s->names_depth][i] = NULL;
    }
    free( s->names_stack[s->names_depth] );
    s->names_stack[s->names_depth] = NULL;

    // Move up
    s->names_depth--;

    // Advance to the next, if we're not already at the end.
    if( s->names_stack[s->names_depth][s->names_i[s->names_depth]] ) {
      s->names_i[s->names_depth]++;
    }

    // Now, do we have a non-NULL stack name?
    if( s->names_stack[s->names_depth][s->names_i[s->names_depth]] ) {
      break;
    }
  }

  return ERR_NOERR;
}

static
error_t go_down(file_find_state_t* s)
{
  error_t err;
  struct stat stats;

  while( 1 ) {
    err = set_curpath(s);
    if( err ) return err;

    // Just starting out with root cur_root..
    err = stat(s->cur_path, &stats);
    if( err ) return ERR_IO_STR("Could not stat file");

    if( ! S_ISDIR(stats.st_mode) ) {
      // OK! Not a directory.
      return ERR_NOERR;
    } else {
      // It's a directory.
      char** names = NULL;
      int names_count = 0;
      char* name = NULL;
      DIR* dir;
      struct dirent* ent;
      int idx;

      // it's a directory!
      dir = opendir(s->cur_path);

      if( ! dir ) return ERR_IO_UNK;

      while( (ent = readdir(dir)) ) {
        if( 0 == strcmp(ent->d_name, ".") ) continue;
        else if( 0 == strcmp(ent->d_name, "..") ) continue;
        name = strdup(ent->d_name);
        if( ! name ) return ERR_MEM;

        err = append_array(&names_count, &names, sizeof(char*), &name);
        if( err ) return err;
      }
      closedir(dir);

      // If we have no names, go up.
      if( names_count == 0 ) {
        // Advance to the next one there, if we're not already at the end.
        if( s->names_stack[s->names_depth][s->names_i[s->names_depth]] ) {
          s->names_i[s->names_depth]++;
        }

        err = go_up(s);
        if( err ) return err;

        if( ! s->names_stack[s->names_depth][s->names_i[s->names_depth]] ) {
          // We're at the end!
          return ERR_NOERR;
        }
      } else {

        // Sort the names
        qsort(names, names_count, sizeof(char*), cmpstringp);


        // Always append a NULL.
        name = NULL;
        err = append_array(&names_count, &names, sizeof(char*), &name);
        if( err ) return err;

        idx = 0;
        s->names_depth++;

        // put names at the end of the names stack, and set index=0.
        if( s->names_depth < s->names_stack_size ) {
          s->names_stack[s->names_depth] = names;
          s->names_i[s->names_depth] = idx;
        } else {
          err = append_array(&s->names_stack_size, &s->names_stack, sizeof(char**), &names);
          if( err ) return err;
          err = append_array(&s->names_i_size, &s->names_i, sizeof(int), &idx);
          if( err ) return err;
        }

        assert( s->names_stack_size == s->names_i_size );
      }
    }
  }
}

error_t init_file_find(file_find_state_t* s, int num_paths, const char** paths)
{
  int total_len;
  error_t err;
  char** root_paths = NULL;
  int idx;

  memset(s, 0, sizeof(file_find_state_t));

  s->names_stack_size = 0;
  s->names_stack = NULL;
  s->names_i_size = 0;
  s->names_i = NULL;

  // always make sure there is a null at the end.
  root_paths = calloc(num_paths+1, sizeof(char*));
  if( ! root_paths ) {
    err = ERR_MEM;
    goto error;
  } 
  total_len = 0;
  for( int i = 0; i < num_paths; i++ ) {
    total_len += strlen(paths[i]);
    root_paths[i] = strdup(paths[i]);
    if( ! root_paths[i] ) {
      err = ERR_MEM;
      goto free_root_paths;
    }
  }

  err = append_array(&s->names_stack_size, &s->names_stack, sizeof(char**), &root_paths);
  if( err ) goto free_root_paths;
  idx = 0;
  err = append_array(&s->names_i_size, &s->names_i, sizeof(int), &idx);
  if( err ) goto error;
 
  // Allocate cur_path.
  total_len += 1024; // leave a bunch of extra room.
  s->path_max = total_len;
  s->cur_path = malloc(total_len);
  if( ! s->cur_path ) goto error;
  s->cur_path[0] = '\0';

  s->names_depth = 0;

  return go_down(s);

free_root_paths:
  for( int i = 0; i < num_paths; i++ ) {
    free(root_paths[i]);
  }
  free(root_paths);

error:
  free_file_find(s);
  return err;
}

error_t reset_file_find(file_find_state_t* s)
{
  error_t err;

  // Move to the end of everything
  for( int d = 0; d <= s->names_depth; d++ ) {
    while( s->names_stack[s->names_depth][s->names_i[d]] ) {
      s->names_i[d]++;
    }
  }

  err = go_up(s);
  if( err ) return err;

  assert( s->names_depth == 0 );

  // now set the root index to 0.
  s->names_i[0] = 0;

  err = go_down(s);
  if( err ) return err;

  return ERR_NOERR;
}

void free_file_find(file_find_state_t* s)
{
  if( s->names_stack ) {
    for( int d = 0; d < s->names_stack_size; d++ ) {
      if( s->names_stack[d] ) {
        for( int i = 0; s->names_stack[d][i]; i++ ) {
          if( s->names_stack[d][i]) free(s->names_stack[d][i]);
          s->names_stack[d][i] = NULL;
        }
        free( s->names_stack[d] );
      }
      s->names_stack[d] = NULL;
    }
    free(s->names_stack);
  }
  s->names_stack = NULL;
  s->names_stack_size = 0;

  if( s->names_i ) {
    free( s->names_i );
  }
  s->names_i = NULL;
  s->names_i_size = 0;

  free( s->cur_path );
  s->cur_path = NULL;
  s->path_max = 0;
}


// *path must be freed by the caller.
// returns NULL when we're done.
error_t file_find_get_next(file_find_state_t* s, char** path)
{
  error_t err;

  if( s->names_depth == 0 &&
      s->names_stack[0][s->names_i[0]] == NULL ) {
    // We're at the end!
    *path = NULL;
    return ERR_NOERR;
  } else {
    *path = strdup(s->cur_path);
    // Continue to get the next path ready.
  }

  //printf("Before advancing\n");
  //print_stack(s);

  // Advance to the next one there, if we're not already at the end.
  if( s->names_stack[s->names_depth][s->names_i[s->names_depth]] ) {
    s->names_i[s->names_depth]++;
  }

  // Now.. if we need to, go up.
  // Go up a level.
  err = go_up(s);
  if( err ) return err;

  //printf("After going up\n");
  //print_stack(s);
  // We're not at the end.

  // Go down if we're not at the end.
  if( s->names_stack[s->names_depth][s->names_i[s->names_depth]] ) {
    // Go down from here.
    err = go_down(s);
    if( err ) return err;
  }

  //printf("After advancing\n");
  //print_stack(s);

  return ERR_NOERR;
}

error_t file_find(file_find_state_t* s, file_find_func_t fun, void* state)
{
  error_t err;
  char* path;

  while( 1 ) {
    path = NULL;
    err = file_find_get_next(s, &path);
    if( err ) return err;

    if( path == NULL ) break;
    fun(path, state);

    free(path);
  }

  return ERR_NOERR;
}

