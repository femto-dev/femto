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

  femto/src/main/results.h
*/
#ifndef _RESULTS_H_
#define _RESULTS_H_

#include "inttypes.h"
#include "buffer.h"
#include "error.h"

typedef struct {
  int64_t doc; // -1 if invalid
  int64_t offset;
} location_info_t;

typedef enum {
  RESULT_TYPE_COUNT = 0,
  RESULT_TYPE_DOCUMENTS = 1,
  RESULT_TYPE_OFFSETS = 2, // doc+offsets == 3
  RESULT_TYPE_DOC_OFFSETS = 3, // document numbers and document offsets
  RESULT_TYPE_REV_OFFSETS = 4, // offsets are sorted largest to least
                               // reader gets negative offsets
} result_type_t;

static inline 
int is_doc_offsets(result_type_t t)
{
  return (t & RESULT_TYPE_DOCUMENTS) && (t & RESULT_TYPE_OFFSETS);
}

/**
  results_t keeps track of a sorted list of non-repeating 
  document numbers and possibly offsets within those documents.
  */
typedef struct {
  result_type_t type;
  int64_t num_documents;
  unsigned char* data;
} results_t;

/**
  * Returns the size of the results data section -
  * useful to append to buffers.
  */
int64_t results_data_size(results_t* results);

/** 
  Print a results... for debugging.
  */
void results_print(FILE* f, results_t* results);

/**
  Returns an empty result set with the specified type -
  for variable initializers.
  */
results_t empty_results(result_type_t type);

/**
  Frees the memory used by a results_t.
  */
void results_destroy(results_t* results);

/** 
  Returns the type for some results.
  */
result_type_t results_type(results_t* results);

/**
  Zeros a results_t structure
  (does not call results_destroy)
  */
void results_clear(results_t* results);
void results_clear_set_type(results_t* results, result_type_t type);

int64_t results_num_results(results_t* results);

/**
  Moves a results set from one to another;
  clears the second.
  If there are results in dst, they are freed with
  results_destroy.
  The results in dst are overwritten
  with the results from src, and src is zeroed.
  */
void results_move(results_t* dst, results_t* src);

error_t results_copy(results_t* dst, results_t* src);

/**
  Returns the maximum size used by num results.
  */
size_t maximum_size_for_results(int64_t num);

/** 
  * Computes the intersection of two results sets.
  */
error_t intersectResults(results_t* leftExpr, 
			 results_t* rightExpr, 
			 results_t* result);
/**
  * Computes the union of two results sets.
  */
error_t unionResults(results_t* leftExpr, 
		     results_t* rightExpr, 
		     results_t* result);
/**
  * Computes leftExpr - rightExpr
  */
error_t subtractResults(results_t* leftExpr, 
		        results_t* rightExpr, 
		        results_t* result);
                        
error_t thenResults(results_t* leftExpr, 
		    results_t* rightExpr, 
		    results_t* result,
                    int distance);
                    
error_t withinResults(results_t* leftExpr, 
		      results_t* rightExpr, 
		      results_t* result,
                      int distance);
typedef struct {
  buffer_t buf;
  int64_t last_document;
  int64_t last_offset;
  int64_t num_documents;
  result_type_t type;
} results_writer_t;

/**
  Initializes a result_writer_t so that it can 
  build up a list of results with calls to 
  append() and then ending with a call to finish().
  */
error_t results_writer_create(results_writer_t* w, result_type_t type);

/**
  Appends a result. This result must be greater than the last
  result appended or ERR_PARAM is returned. 
  */
error_t results_writer_append(results_writer_t* w, int64_t document, int64_t offset);

/* Finishes a result writer. Stores the results in out.
   Note that these results must be destroyed when they
   are done being used (with results_destroy()).
   results_writer_destroy should still be called.
   If out is a set of results that has not been destroyed,
   it will be destroyed before being replaced.
   */
error_t results_writer_finish(results_writer_t* w, results_t* out);

/* Frees memory created by results_writer_create.
   */
void results_writer_destroy(results_writer_t* w);

/**
  Assuming that we have a results_t already, write it to a buffer.
  Appends the data section to an existing buffer.
  Works with DOCUMENT-type results only.
  */
error_t results_write_buffer(buffer_t* buf, results_t* r);

/**
  Creates a results_t for some unsorted results. This results_t must be 
  freed with results_destroy. Works with DOCUMENT-type results only.
  If r has results in it already, they are freed.

  Sorts the results array in place and discards duplicates. Then
  saves the results in r.
  */
error_t results_create_sort(results_t* r, int64_t num, int64_t* results);
/**
  * works with DOCUMENT_OFFSET results only

    Sorts the results array in place and discards duplicates. Then
    saves the results in r.
  */
error_t results_create_sort_locations(results_t* r, int64_t num, location_info_t* results);

typedef struct {
  buffer_t buf;
  int64_t last_document;
  int64_t last_offset;
  int64_t num_left;
  result_type_t type;
  unsigned char is_in_offsets;
} results_reader_t;

/* Initializes a results_reader_t so that it can go through
   a list of results (created with results_writer). This reader
   will read the results pointed to by in.
   */
error_t results_reader_create(results_reader_t* r, results_t* in);

/* Reads the next result from a results_reader.
   Returns 0 if there was no result to read, or 1 if a 
   result is returned in result.
   */
int results_reader_next(results_reader_t* r, int64_t* document, int64_t* offset);

/* Frees the memory created in results_reader_create.
   */
void results_reader_destroy(results_reader_t* r);

#endif
