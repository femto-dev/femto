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

  femto/src/dcx_cc/index_tool_support.h
*/
#ifndef _INDEX_TOOL_SUPPORT_H_
#define _INDEX_TOOL_SUPPORT_H_

#include <inttypes.h>

// This function is available to call.
extern
void index_tool_usage(void);

/* To use index_tool with custom input,
 * implement the following functions and link them in.
 * The default index tool uses the functions in index_tool_files.c
 */


/* Print out usage information for this index tool adapter */
extern
void its_usage(FILE* out);


/*
 * In setting up, we call this function to
 * initialize the document reader.
 * It should return a negative number for an error,
 * and >=0 for success.
 *
 * This function should print out usage if inappropriate
 * arguments are given. It can call index_tool_usage()
 * to print out the usage statement for the index tool.
 *
 * The arguments are the command line arguments not
 * handled by index_tool.c (ie the file/directory names
 * to index).
 */
extern
int its_use_arguments(int num_args, const char** args);

/*
 * In pass 1, get_doc_info is called for each document
 * (doc_num just is incremented each time) until it
 * returns 0 to indicate that there are no more documents.
 * It should return 1 if it got the info for a document,
 * or negative for an error.
 *
 * doc_num is the document number, starting counting from 0
 * *doc_len_out         is a place to store the document length
 * *doc_info_len_out    is a place to store the document URL length
 *                      (or whatever you want to be returned with
 *                       the document when it matches).
 * *doc_info_out        is a place to store the document URL. This will
 *                      be freed by a call to its_free_doc.
 *                      by the caller.
 * *num_doc_headers_out is a place to store document header
 *                      lengths, which will be indexed with
 *                      the document in a special header section
 * *doc_header_lens_out is an array of document header lengths
 *                      for this document (of size *num_doc_headers_out).
 *                      It can be NULL or just never set if
 *                      *num_doc_headers_out is 0. This array should
 *                      be allocated with malloc (if it is not NULL).
 *                      and will be freed by the caller.
 */
extern
int its_get_doc_info(int64_t doc_num,
                     int64_t* doc_len_out,
                     int64_t* doc_info_len_out,
                     unsigned char** doc_info_out,
                     int64_t* num_doc_headers_out, 
                     int64_t** doc_header_lens_out);

/* This is called between the passes; after pass 1 is 
 * finished but before pass 2 begins.
 *
 * Return a negative number for error, or >=0 for success.
 */
extern
int its_switch_passes(void);

/*
 * In pass 2, get_doc is called for each document
 * that we got document info for in the previous pass, in
 * the same order (ie increasing doc_num).
 *
 * This function should return 1 for success,
 * 0 if we ran out of documents, or negative for an error.
 *
 * doc_num is the document number, starting counting from 0
 * doc_len is the document length returned by pass 1
 * doc_info_len is the document URL length returned by pass 1
 * doc_info is the document URL returned by pass 1. It might not
 *          be followed by a NULL character, so be sure to use
 *          doc_info_len (instead of just assuming you have a C string)
 * *num_doc_headers is the number of document headers requested for this
 *                  document. It should match the number from pass 1.
 * *doc_header_lens is the length of each document header requested in
 *                  for this document. It can be set to NULL if
 *                  *num_doc_headers is 0. It should match what
 *                  was returned in pass 1. This array should be allocated
 *                  with malloc (if it is not set to NULL) and will be
 *                  freed by the caller.
 * *doc_headers_out is an array of size num_doc_headers, where each element i
 *                  has its length determined by (*doc_header_lens)[i].
 *                  Here the actual document headers are stored.
 * *doc_contents_out is the actual document body. It could be mmap'd
 *                   or malloc'd; it will be freed by a call to
 *                   its_free_doc.
 */
extern
int its_get_doc(int64_t doc_num,
                int64_t* doc_len_out,
                int64_t* doc_info_len_out,
                unsigned char** doc_info_out,
                int64_t* num_doc_headers_out,
                int64_t** doc_header_lens_out,
                unsigned char*** doc_headers_out,
                unsigned char** doc_contents_out);

/*
 * Called in pass 1 and pass 2 to release resources.
 * Frees the document info, the header lengths array,
 * the document headers themselves, and the document contents.
 * 
 * If any of the input arguments are NULL, they should not
 * be freed and should be ignored. In particular, in pass 1,
 * this function will be called with NULL doc_headers and doc_contents.
 *
 * returns >=0 for success, negative for error.
 */
extern
int its_free_doc(int64_t doc_num,
                 int64_t doc_len,
                 int64_t doc_info_len,
                 unsigned char* doc_info,
                 int64_t num_doc_headers,
                 int64_t* doc_header_lens,
                 unsigned char** doc_headers,
                 unsigned char* doc_contents);

/* Called once when we're all done (so we know we're not leaking memory).
 */
extern
void its_cleanup(void);

#endif

