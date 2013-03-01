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

  femto/src/main/bwt_creator.c
*/

#include "bwt.h"
#include "index.h"
#include "file_find.h"

#include "buffer_funcs.h"

#include "bwt_creator.h"
#include "bwt_writer.h"
#include "timing.h"


// handy interface function
/* Saves the BWT for some prepared text. 
   */
error_t save_prepared_bwt(prepared_text_t* p,
	                  int mark_period,
                          FILE* bwt_out,
                          int chunk_size, FILE* map_out,
                          int verbose)
{
  suffix_array_t sa;
  error_t err;
  int64_t num_chars, num_docs;
 
  assert( bwt_out );

  err = prepared_num_docs( p, &num_docs);
  if( err ) return err;
  err = prepared_num_chars( p, &num_chars);
  if( err ) return err;

  // now we've got the prepared text for our documents.
  // Do the next thing - suffix sort it.
  if( verbose ) printf("Suffix sorting\n");
  start_clock();
  err = suffix_sort(p, &sa);
  if( err ) return err;
  stop_clock();
  if( verbose ) print_timings("suffix sorting bytes", num_chars);

  // now construct the output file.
  if( verbose ) printf("Outputting BWT\n");
  start_clock();
  {
    sa_reader_t r;
    bwt_writer_t bwt;
    bwt_document_map_writer_t aux_writer;
    int64_t use_offset;

    r.mark_period = mark_period;
    r.p = p;
    r.sa = &sa;
    r.state = NULL;
    r.offset = -1;
    r.L = INVALID_ALPHA;
    err = init_sa_reader(&r);
    if( err ) return err;

    // write the chunk start.
    if( map_out ) {
      err = bwt_document_map_start_write(&aux_writer, map_out, chunk_size,
	      num_chars,
	      num_docs);
      if( err ) return err;
    }

    err = bwt_begin_write_easy(&bwt, bwt_out,
                               p, mark_period);
    if( err ) return err;

    while(read_sa_bwt(&r)) {
      // check to see if we got a valid character - 
      // ignore if it's one of the 9 extra EOF chars.
      // now we've got an offset and an L.
      if( r.mark ) {
        use_offset = r.offset;
      } else {
        use_offset = -1;
      }

      err = bwt_append(&bwt, r.L, use_offset);
      if( err ) return err;

      if( map_out ) {
        err = bwt_document_map_append(&aux_writer, r.doc);
        if( err ) return err;
      }
    }

    // we're done appending.
    err = bwt_finish_write(&bwt);
    if( err ) return err;
    

    if( map_out ) {
      err = bwt_document_map_finish_write(&aux_writer);
      if( err ) return err;
    }

    free_sa_reader(&r);  
  }
  stop_clock();
  if( verbose ) print_timings("outputting bytes", num_chars);

  free_suffix_array(&sa);

  stop_clock();

  if( verbose ) print_timings("BWT-transform bytes", num_chars);
  
  return ERR_NOERR;
}
