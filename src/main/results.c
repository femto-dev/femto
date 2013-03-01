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

  femto/src/main/results.c
*/
#include "results.h"
#include <stdlib.h>
#include "util.h"

#include "buffer_funcs.h"

void results_clear(results_t* results)
{
  results->data = NULL;
  results->num_documents = 0;
  //memset(results, 0, sizeof(results_t));
}


void results_clear_set_type(results_t* results, result_type_t type)
{
  results_clear(results);
  results->type = type;
}

results_t empty_results(result_type_t type)
{
  results_t ret;
  results_clear_set_type(&ret, type);
  return ret;
}

void results_destroy(results_t* results)
{
  if( results->data ) free(results->data);
  results_clear_set_type(results, results->type);
}

result_type_t results_type(results_t* results)
{
  return results->type;
}

void results_move(results_t* dst, results_t* src)
{
  if( dst->data ) results_destroy( dst );
  *dst = *src;
  results_clear(src);
}

int64_t results_data_size(results_t* results)
{
  results_reader_t r;
  int64_t document, offset;
  int64_t ret = 0;
  error_t err;
  err = results_reader_create(&r, results);
  if( err ) return 0;
  while( results_reader_next(&r, &document, &offset) ) {
    ; // do nothing.
  }
  ret = r.buf.pos;
  results_reader_destroy(&r);
  return ret;
}

void results_print(FILE* f, results_t* results)
{
  results_reader_t r;
  int64_t document, offset;
  error_t err;
  err = results_reader_create(&r, results);
  if( err ) {
    fprintf(f, "Results print error: %s", err_string(err));
  }
  while( results_reader_next(&r, &document, &offset) ) {
    fprintf(f, "Result doc=%" PRIi64 " off=%" PRIi64"\n", document, offset);
  }
  results_reader_destroy(&r);
}

error_t results_copy(results_t* dst, results_t* src)
{
  int64_t size;
  if( dst->data ) results_destroy( dst );
  *dst = *src;
  size = results_data_size(src);
  dst->data = malloc(size);
  if( ! dst->data ) {
    dst->num_documents = 0;
    return ERR_MEM;
  }
  memcpy(dst->data, src->data, size);
  return ERR_NOERR;
}

size_t maximum_size_for_results(int64_t num)
{
  return 3*sizeof(int64_t)*num;
}

int64_t results_num_results(results_t* results)
{
  results_reader_t r;
  int64_t document, offset;
  int64_t ret = 0;
  error_t err;
  err = results_reader_create(&r, results);
  if( err ) return 0;
  while( results_reader_next(&r, &document, &offset) ) {
    ret++;
  }
  results_reader_destroy(&r);
  return ret;
}

// returns last
static inline
error_t append_document_internal(buffer_t* buf, int64_t result,
                                 int64_t* last_written, int64_t* num_written)
{
  if( *num_written == 0 ) *last_written = 0;

  // we're going to add one to result so that we can encode an initial
  // zero.
  result++;

  // only append results that are greater than the last written.
  if( result <= *last_written ) return ERR_PARAM;

  buffer_encode_gamma(buf, result - *last_written);

  *last_written = result;
  *num_written = *num_written + 1;

  return ERR_NOERR;
}

error_t results_write_buffer(buffer_t* buf, results_t* results)
{
  int64_t last, num_written;
  int64_t size;

  last = num_written = 0;

  // first, find the length of the results
  size = results_data_size(results);

  // next, append those bytes.
  memcpy(&buf->data[buf->len], results->data, size);

  return ERR_NOERR;
}

error_t results_create_sort(results_t* r, int64_t num, int64_t* results)
{
  error_t err;
  int64_t unique;
  results_writer_t writer;

  // sort and dedup them!
  unique = sort_dedup(results, num, sizeof(int64_t), compare_int64);

  // store them in a results set!
  err = results_writer_create(&writer, RESULT_TYPE_DOCUMENTS);
  if( err ) return err;

  for( int64_t i = 0; i < unique; i++ ) {
    err = results_writer_append(&writer, results[i], 0);
    if( err ) goto error;
  }

  err = results_writer_finish(&writer, r);
  if( err ) goto error;

  err = ERR_NOERR;

error:
  results_writer_destroy(&writer);
  return err;
}

int compare_location_info(const void* aP, const void* bP)
{
  location_info_t* a = (location_info_t*) aP;
  location_info_t* b = (location_info_t*) bP;

  if( a->doc < b->doc ) return -1;
  else if ( a->doc > b->doc ) return 1;
  else {
    if( a->offset < b->offset ) return -1;
    if( a->offset > b->offset ) return 1;
    else return 0;
  }
}

error_t results_create_sort_locations(results_t* r, int64_t num, location_info_t* results)
{
  error_t err;
  int64_t unique;
  results_writer_t writer;

  // sort and dedup them!
  unique = sort_dedup(results, num, sizeof(location_info_t), compare_location_info);

  // store them in a results set!
  err = results_writer_create(&writer, RESULT_TYPE_DOC_OFFSETS);
  if( err ) return err;

  for( int64_t i = 0; i < unique; i++ ) {
    err = results_writer_append(&writer, results[i].doc, results[i].offset);
    if( err ) goto error;
  }

  err = results_writer_finish(&writer, r);
  if( err ) goto error;

  err = ERR_NOERR;

error:
  results_writer_destroy(&writer);
  return err;
}

error_t results_writer_create(results_writer_t* w, result_type_t type)
{
  int start_size = 128;
  unsigned char* data = malloc(start_size);
  if( ! data ) return ERR_MEM;

  w->buf = build_buffer(start_size, data);
  w->last_document = 0;
  w->last_offset = 0;
  w->num_documents = 0;
  w->type = type;

  bsInitWrite(& w->buf);

  return ERR_NOERR;
}

error_t results_writer_append(results_writer_t* w, int64_t document, int64_t offset )
{
  error_t err;

  // make sure there's room for a gamma-encoded value in the results.
  err = buffer_extend(&w->buf, 5*sizeof(int64_t));
  if( err ) return err;

  if( w->type == RESULT_TYPE_DOCUMENTS ) {
    // append the document number.
    err = append_document_internal(&w->buf, document, &w->last_document, &w->num_documents);
    if( err ) return err;
  }

  if( is_doc_offsets(w->type) ) {
    if( w->last_document != document+1 ) {
      if( w->num_documents != 0 ) {
        // append the "end of offsets" value.
        buffer_encode_gamma(&w->buf, 1 );
      }
      // append the document number.
      err = append_document_internal(&w->buf, document, &w->last_document, &w->num_documents);
      if( err ) return err;
      // start the offsets at 0.
      w->last_offset = 0;
    }
    // append the offset.
    offset++; // add one to make numbers count from 1.
    if( w->last_offset == 0 && (w->type & RESULT_TYPE_REV_OFFSETS) ) {
      // the first offset should be <= 0.
      if( offset > 1 ) return ERR_PARAM;
      // we're recording negative offsets...
      // so write a "2" if the input was "0" 
      // so write a "3" if the input was "-1" 
      buffer_encode_gamma(&w->buf, 3 - offset);
    } else {
      if( offset <= w->last_offset ) return ERR_PARAM;
      buffer_encode_gamma(&w->buf, 1 + offset - w->last_offset);
    }
    w->last_offset = offset;
  }

  return ERR_NOERR;
}

error_t results_writer_finish(results_writer_t* w, results_t* out)
{
  if( w->type & RESULT_TYPE_OFFSETS ) {
    // encode the "end of offsets" value
    buffer_encode_gamma(&w->buf, 1);
  }
  bsFinishWrite( & w->buf );

  if( out->data ) results_destroy(out);

  out->data = w->buf.data;
  out->num_documents = w->num_documents;
  out->type = w->type;

  // clear the buffer.
  w->buf = build_buffer(0, NULL);
  w->num_documents = w->last_document = 0;
  
  return ERR_NOERR;
}

/* Frees memory created by results_writer_create.
   */
void results_writer_destroy(results_writer_t* w)
{
  // normally the buffer is zeroed by finish(), but
  // if there was an error it might not be.
  free(w->buf.data);
  w->buf.data = NULL;
}

error_t results_reader_create(results_reader_t* r, results_t* in)
{
  // the maximum size of the buffer isn't necessarily right,
  // but we don't check that in decode_gamma anyways, and also
  // we'll know if we're going off of the end from num_left.
  r->buf = build_buffer(0, in->data);

  r->last_document = 0;
  r->last_offset = 0;
  r->num_left = in->num_documents;
  r->type = in->type;
  r->is_in_offsets = 0;

  bsInitRead(& r->buf);

  return ERR_NOERR;
}

/* Reads the next result from a results_reader.
   Returns 0 if there was no result to read, or 1 if a 
   result is returned in result.
   */
int results_reader_next(results_reader_t* r, int64_t* document, int64_t* offset)
{
  if( r->type == RESULT_TYPE_DOCUMENTS ) {
    if( r->num_left > 0 ) {
      // read a result.
      r->last_document += buffer_decode_gamma(&r->buf);
      r->num_left--;

      // undo the adding one to handle initial zero.
      *document = r->last_document - 1;
      if( offset ) *offset = 0;
      return 1;
    } else {
      return 0;
    }
  }

  if( is_doc_offsets(r->type) ) {
    int64_t tmp;
    int repeat = 1;
    do {
      if( ! r->is_in_offsets ) {
        if( r->num_left > 0 ) {
          // read a new document number
          r->last_document += buffer_decode_gamma(&r->buf);
          r->num_left--;

          // undo the adding one to handle initial zero.
          //printf("decoded document %i\n", (int) r->last_document - 1);

          // next, read an offset.
          r->is_in_offsets = 1;
          r->last_offset = 0;
          repeat = 0; // we'll be done after we read the offset.
        } else {
          return 0; // no more documents left.
        }
      }

      *document = r->last_document - 1;

      if( r->is_in_offsets ) {
        // try reading an offset.
        tmp = buffer_decode_gamma(&r->buf);
        //printf("decoded offset gamma %i\n", (int) tmp);
        if( tmp == 1 ) {
          // "end of offsets"
          r->is_in_offsets = 0;
          // doesn't set done!
        } else {
          tmp--; // take away 1 from delta for end-of-offsets.
          if( r->last_offset == 0 && (r->type & RESULT_TYPE_REV_OFFSETS) ) {
            r->last_offset = 2 - tmp;
          } else {
            r->last_offset += tmp;
          }
          //printf("got offset %i\n", (int) r->last_offset - 1);
          if( offset ) {
            *offset = r->last_offset - 1; 
             repeat = 0;
          }
        }
      }
    } while( repeat );

    return 1;
  }

  return 0;
}

/** 
  *Frees the memory created in results_reader_create.
  */
void results_reader_destroy(results_reader_t* r)
{
  bsFinishRead(& r->buf);
}

error_t intersectResults(results_t* leftExpr, 
			 results_t* rightExpr, 
			 results_t* result)
{
  int64_t left; // Current document for left & right terms of expression
  int64_t right;
  int64_t leftReadResult; //Ints representing whether or not the read was
  int64_t rightReadResult; //successful or not.
  results_reader_t leftReader; 
  results_reader_t rightReader;
  results_writer_t writer; 
  error_t error;
  error = results_reader_create(&leftReader,leftExpr);
  if(error)
  {
    return error;
  }
  error = results_reader_create(&rightReader,rightExpr);
  if(error)
  {
    return error;
  }
  error = results_writer_create(&writer, RESULT_TYPE_DOCUMENTS);
  if(error)
  {
    return error; 
  }
  leftReadResult = results_reader_next(&leftReader, &left, NULL);
  rightReadResult = results_reader_next(&rightReader, &right, NULL);
  while( leftReadResult && rightReadResult )
  {
    if(left < right)
    {
      leftReadResult = results_reader_next(&leftReader, &left, NULL);
    }
    else if ( left > right )
    {
      rightReadResult = results_reader_next(&rightReader, &right, NULL);
    }
    else /* left == right */
    {
      error = results_writer_append(&writer, left, 0);
      if(error)
      {
        return error;
      }
      leftReadResult = results_reader_next(&leftReader, &left, NULL);
      rightReadResult = results_reader_next(&rightReader, &right, NULL);  
      
    }
  } 
  error = results_writer_finish( &writer, result );
  if(error)
  {
    return error; 
  }
  results_writer_destroy(&writer);
  results_reader_destroy(&leftReader);
  results_reader_destroy(&rightReader);
  return ERR_NOERR;
}

error_t unionResults(results_t* leftExpr, 
		     results_t* rightExpr, 
		     results_t* result)
{
  int64_t leftDoc; // Current document for left & right terms of expression
  int64_t rightDoc;
  int64_t leftOffset;
  int64_t rightOffset;
  int offsets;
  int64_t leftReadResult; //Ints representing whether or not the read was
  int64_t rightReadResult; //successful or not.
  results_reader_t leftReader; 
  results_reader_t rightReader;
  results_writer_t writer; 
  error_t error;
  
  if(leftExpr->type & RESULT_TYPE_OFFSETS )
  {
    offsets = 1;
  }
  else
  {
    offsets = 0;
  }
  
  error = results_reader_create(&leftReader,leftExpr);
  if(error)
  {
    return error;
  }
  error = results_reader_create(&rightReader,rightExpr);
  if(error)
  {
    return error;
  }

  error = results_writer_create(&writer, leftExpr->type);
  if(error)
  {
    return error; 
  }
  
  
  if(leftExpr->type != rightExpr->type)
  {
    printf("DIFFERENT TYPES!\n");
    printf("left:%i right:%i\n",leftExpr->type,rightExpr->type);
    return ERR_PARAM;
  }
  
  
  leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
  rightReadResult = results_reader_next(&rightReader, &rightDoc, &rightOffset);
  while( leftReadResult || rightReadResult )
  {
    if(!leftReadResult)
    {
      error = results_writer_append(&writer, rightDoc, rightOffset);
      rightReadResult = results_reader_next(&rightReader, 
                                            &rightDoc,
                                            &rightOffset);
      if(error)
      {
        return error;
      }
      
    }
    else if(!rightReadResult)
    {
      error = results_writer_append(&writer, leftDoc, leftOffset);
      leftReadResult = results_reader_next(&leftReader, 
                                           &leftDoc,
                                           &leftOffset);  
      if(error)
      {
        return error;
      }
      
    }
    else if( leftDoc < rightDoc)
    {
      error = results_writer_append(&writer, leftDoc, leftOffset);
      if(error)
      {
        return error;
      }
      leftReadResult = results_reader_next(&leftReader, 
                                           &leftDoc,
                                           &leftOffset);      
      
    }
    else if ( leftDoc > rightDoc )
    {
      error = results_writer_append(&writer, rightDoc, rightOffset);
      if(error)
      {
        return error;
      }
      rightReadResult = results_reader_next(&rightReader,
                                            &rightDoc,
                                            &rightOffset);
      
    }
    else /* leftDoc == rightDoc */
    {
      if(offsets)
      {
        if(leftOffset < rightOffset)
        {
          error = results_writer_append(&writer, leftDoc, leftOffset);
          if(error)
          {
            return error;
          }
          leftReadResult = results_reader_next(&leftReader,
                                               &leftDoc,
                                               &leftOffset);
        }
        else if( leftOffset > rightOffset )
        {
          error = results_writer_append(&writer, rightDoc, rightOffset);
          if(error)
          {
            return error;
          }
          rightReadResult = results_reader_next(&rightReader,
                                                &rightDoc,
                                                &rightOffset);
        } else {
          // both are the same.
          error = results_writer_append(&writer, leftDoc, leftOffset);
          if(error)
          {
            return error;
          }
          leftReadResult = results_reader_next(&leftReader,
                                               &leftDoc,
                                               &leftOffset);
          rightReadResult = results_reader_next(&rightReader,
                                                &rightDoc,
                                                &rightOffset);
        }
      }
      else
      {
        //Since not offset-based, doesnt matter which side we use since they
        //are equal.
        error = results_writer_append(&writer, leftDoc, 0);
        if(error)
        {
          return error;
        }
        leftReadResult = results_reader_next(&leftReader,
                                             &leftDoc,
                                             &leftOffset);
        rightReadResult = results_reader_next(&rightReader,
                                              &rightDoc,
                                              &rightOffset);
      }
    }
  } 
  error = results_writer_finish( &writer, result );
  if(error)
  {
    return error; 
  }
  results_writer_destroy(&writer);
  results_reader_destroy(&leftReader);
  results_reader_destroy(&rightReader);
  return ERR_NOERR;
}

error_t subtractResults(results_t* leftExpr, 
		        results_t* rightExpr, 
		        results_t* result)
{
  int64_t left; // Current document for left & right terms of expression
  int64_t right; 
  int64_t leftReadResult; //Ints representing whether or not the read was
  int64_t rightReadResult; //successful or not.
  results_reader_t leftReader; 
  results_reader_t rightReader;
  results_writer_t writer; 
  error_t error;
  error = results_reader_create(&leftReader,leftExpr);
  if(error)
  {
    return error;
  }
  error = results_reader_create(&rightReader,rightExpr);
  if(error)
  {
    return error;
  }
  error = results_writer_create(&writer, RESULT_TYPE_DOCUMENTS);
  if(error)
  {
    return error; 
  }
  
  leftReadResult = results_reader_next(&leftReader, &left, NULL);
  rightReadResult = results_reader_next(&rightReader, &right, NULL);
  while( leftReadResult )
  {
    if( !rightReadResult || ( left < right ) )
    {
      error = results_writer_append(&writer, left,0);
      if(error)
      {
        return error;
      }
      leftReadResult = results_reader_next(&leftReader, &left, NULL);
    }
    else if ( left > right )
    {
      rightReadResult = results_reader_next(&rightReader, &right, NULL);
    }
    else /* left == right */
    {
      leftReadResult = results_reader_next(&leftReader, &left, NULL);
      rightReadResult = results_reader_next(&rightReader, &right, NULL);
    }
  } 
  error = results_writer_finish( &writer, result );
  if(error)
  {
    return error; 
  }
  results_writer_destroy(&writer);
  results_reader_destroy(&leftReader);
  results_reader_destroy(&rightReader);
  
  return ERR_NOERR;
}

error_t thenResults(results_t* leftExpr, 
		    results_t* rightExpr, 
		    results_t* result,
                    int distance)
{
  int64_t leftDoc; // Current document for left & right terms of expression
  int64_t rightDoc;
  int64_t leftOffset; // Document offsets for results being evaluated
  int64_t rightOffset;
  int64_t width; //Distance between two offsets within the same doc (right - left)
  int64_t leftReadResult; //Ints representing whether or not the read was
  int64_t rightReadResult; //successful or not.
  results_reader_t leftReader; 
  results_reader_t rightReader;
  results_writer_t writer; 
  error_t error;
  int positive_distance;
  int64_t minOffset;
  if( distance > 0 ) positive_distance = distance;
  else positive_distance = - distance;
  
  error = results_reader_create(&leftReader,leftExpr);
  if(error)
  {
    return error;
  }
  error = results_reader_create(&rightReader,rightExpr);
  if(error)
  {
    return error;
  }
  if( ! is_doc_offsets(leftExpr->type) ) {
    // the expressions must both be doc-offsets
    return ERR_PARAM;
  }
  if( ! is_doc_offsets(rightExpr->type) ) {
    // the expressions must both be doc-offsets
    return ERR_PARAM;
  }
  if( leftExpr->type != rightExpr->type ) {
    return ERR_PARAM;
  }

  error = results_writer_create(&writer,leftExpr->type);
  if(error)
  {
    return error; 
  }
  leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
  rightReadResult = results_reader_next(&rightReader, &rightDoc,&rightOffset);
  while( leftReadResult && rightReadResult )
  { 
    
    if( leftDoc < rightDoc )
    {
      leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
    } 
    else if( leftDoc > rightDoc )
    {
      rightReadResult = results_reader_next(&rightReader, &rightDoc,&rightOffset);
    }
    else 
    {
      width = rightOffset - leftOffset;
      
      // minOffset is the minimum of the two
      if ( rightOffset < leftOffset ) minOffset = rightOffset;
      else minOffset = leftOffset;
      
      if(distance < 0)
      {
        width = - width;
      }
      if(0 < width && width <= positive_distance)
      {
        //append and advance leftmost(minimum) offset
        //which doc used  when appending doesnt matter since they are equal
        error = results_writer_append(&writer, leftDoc, minOffset);
        if(error)
        {
          return error;
        }
      }
      if ( rightOffset < leftOffset )
      {
        //advance right
        rightReadResult = results_reader_next(&rightReader, &rightDoc, &rightOffset);
      }
      else /* width > distance */
      {
        //advance left
        leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
      }
    
    }

  }
  error = results_writer_finish( &writer, result );
  if(error)
  {
    return error; 
  }
  results_writer_destroy(&writer);
  results_reader_destroy(&leftReader);
  results_reader_destroy(&rightReader);
  
  return ERR_NOERR;
}


error_t withinResults(results_t* leftExpr, 
		      results_t* rightExpr, 
		      results_t* result,
                      int distance)
{
  int64_t leftDoc; // Current document for left & right terms of expression
  int64_t rightDoc;
  int64_t leftOffset; // Document offsets for results being evaluated
  int64_t rightOffset;
  int64_t width; //Distance between two offsets within the same doc (right - left)
  int64_t leftReadResult; //Ints representing whether or not the read was
  int64_t rightReadResult; //successful or not.
  results_reader_t leftReader; 
  results_reader_t rightReader;
  results_writer_t writer; 
  error_t error;
  int positive_distance;
  int64_t minOffset;
  
  if( distance > 0 ) positive_distance = distance;
  else positive_distance = - distance;
  
  error = results_reader_create(&leftReader,leftExpr);
  
  if(error)
  {
    return error;
  }
  error = results_reader_create(&rightReader,rightExpr);
  if(error)
  {
    return error;
  }

  if( ! is_doc_offsets(leftExpr->type) ) {
    // the expressions must both be doc-offsets
    return ERR_PARAM;
  }
  if( ! is_doc_offsets(rightExpr->type) ) {
    // the expressions must both be doc-offsets
    return ERR_PARAM;
  }
  if( leftExpr->type != rightExpr->type ) {
    return ERR_PARAM;
  }

  error = results_writer_create(&writer,leftExpr->type);
  if(error)
  {
    return error; 
  }
  
  leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
  rightReadResult = results_reader_next(&rightReader, &rightDoc,&rightOffset);
  while( leftReadResult && rightReadResult )
  { 
    if( leftDoc < rightDoc )
    {
      leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
    } 
    else if( leftDoc > rightDoc )
    {
      rightReadResult = results_reader_next(&rightReader, &rightDoc,&rightOffset);
    }
    else 
    {
      width = rightOffset - leftOffset;
      if(width < 0)
      {
        width = -width;
      }
      // minOffset is the minimum of the two
      if ( rightOffset < leftOffset ) minOffset = rightOffset;
      else minOffset = leftOffset;
      
      if(width <= positive_distance)
      {
        //append and advance leftmost(minimum) offset
        //which doc used doesnt matter since they are equal
        error = results_writer_append(&writer, leftDoc, minOffset);
        if(error)
        {
          return error;
        }
      }
      if ( rightOffset < leftOffset )
      {
        //advance right
        rightReadResult = results_reader_next(&rightReader, &rightDoc, &rightOffset);
      }
      else /* width > distance */
      {
        //advance left
        leftReadResult = results_reader_next(&leftReader, &leftDoc, &leftOffset);
      }
    
    }

  } 
  error = results_writer_finish( &writer, result );
  if(error)
  {
    return error; 
  }
  results_writer_destroy(&writer);
  results_reader_destroy(&leftReader);
  results_reader_destroy(&rightReader);
  
  return ERR_NOERR;
}
