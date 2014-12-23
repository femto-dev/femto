/*
  (*) 2011-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/scan_tool.cc
*/

#include <cstdio>
#include <cstdlib>
#include <cassert>

#include <vector>
#include <string>

extern "C" {
#include "sequential_search.h"
#include "index_tool_support.h"
}

char* cmd_name;

#include <re2/re2.h>


error_t seq_compile_regexp_from_ast(void** ret, struct ast_node* ast_node)
{
  re2::RE2::Options opts;
  opts.set_utf8(false);

  // Condense the AST so that we have a regexp printed out...
  char * str = ast_to_string(ast_node, 1 /* noncapturing groups */, 0 /* usequotes */);

  re2::RE2* matcher = new re2::RE2(str, opts);
  if( ! matcher->ok() ) {
    fprintf(stderr, "Error creating RE2 pattern '%s': %s\n", str, matcher->error().c_str());
    delete matcher;
    *ret = NULL;
    return ERR_FORMAT_STR("Could not compile regexp");
  }
  *ret = matcher;

  return ERR_NOERR;
}

// On input, *start_offset is the last location of a match.
// returns 0 if there was no match
// returns 1 if we got a match in *start_offset to *end_offset
// returns negative for an error.
int seq_match_regexp(void* regexp,
                 const unsigned char* subject, size_t subject_length,
                 /* OUT -- a match */ size_t* match_offset, size_t* match_len)
{
  re2::RE2* matcher = (re2::RE2*) regexp;
  re2::StringPiece text((const char*) subject, subject_length);
  re2::StringPiece match;

  //printf("Seq searching %p %li\n", subject, (long) subject_length);

  if( matcher->Match(text, 0, subject_length,
                     RE2::UNANCHORED,
                     &match, 1) ) {
    *match_offset = ((const unsigned char*) match.data()) - subject;
    *match_len = match.length();
    //printf("Returning match offset %li length %li\n", *match_offset, *match_len);
    return 1;
  } else {
    return 0;
  }
}

void seq_free_regexp(void* regexp)
{
  re2::RE2* matcher = (re2::RE2*) regexp;
  delete matcher;
}

void index_tool_usage(void)
{
  printf("Usage: %s [options] [path] [path] ... -- <pattern> <pattern> ... \n", cmd_name);
  printf("Usage: %s [options] path <pattern>\n", cmd_name);
  printf("where path is: ");
  its_usage(stdout);
  printf(" where options include:\n");
  printf(" -v or --verbose print extra verbose output\n");
  printf(" --max_results <number> set the maximum number of results\n");
  printf(" --offsets request document offsets\n");
  printf(" --count Ask for only the number of results\n");
  printf(" --matches Show strings matching approximate search and/or regular expression\n");
  printf(" --output <filename> output query results to file instead of stdout\n");
  printf(" --null seperate output lines with 0 bytes instead of newlines\n");
  printf(" --icase make the search case-insensitive\n");
  exit(-1);
}

int main( int argc, char** argv )
{
  std::vector<std::string> input_paths;
  std::vector<std::string> patterns;
  int rc;

  //int has_dashdash = 0;
  int on_patterns = 0;
  int last_pat = 1;

  int verbose = 0;
  int q;

  const char** patarr;
  int npatterns;

  long long int chunk_size = 1024*1024;
  int offsets = 0;
  int count = 0;
  int match = 0;
  char* output_fname = NULL;
  FILE* out = stdout;
  char sep = '\n';
  int icase = 0;
  result_type_t type;
  long long int total_matches = 0;

  const char* prefix = "";

  sequential_search_state_t state;
  results_t* results = NULL;
  int64_t* counts = NULL;
  seq_search_match_keyvalue_t** matches = NULL;
  size_t* nmatches = NULL;
  error_t err;


  cmd_name = argv[0];

  for( int i = 1; i < argc; i++ ) {
    if( 0 == strcmp(argv[i], "--") ) {
      //has_dashdash = 1;
    } else if( argv[i][0] == '-' ) {
      // OK it's an option
    } else {
      last_pat = i;
    }
  }

  for( int i = 1; i < argc; i++ ) {
    if( i == last_pat ) on_patterns = 1;

    if( 0 == strcmp(argv[i], "-v" ) ||
        0 == strcmp(argv[i], "--verbose") ) {
      verbose++;
    } else if( 0 == strcmp(argv[i], "--max_results") ) {
      // set the chunk size
      i++; // pass the -c
      sscanf(argv[i], "%lli", &chunk_size);
    } else if( 0 == strcmp(argv[i], "--offsets") ) {
      offsets = 1;
    } else if( 0 == strcmp(argv[i], "--count") ) {
      count = 1;
    } else if( 0 == strcmp(argv[i], "--matches") ) {
      match = 1;
      count = 1;
    } else if( 0 == strcmp(argv[i], "--output") ) {
      i++; // pass --output
      output_fname = argv[i];
    } else if( 0 == strcmp(argv[i], "--null") ) {
      sep = '\0';
    } else if( 0 == strcmp(argv[i], "--icase") ) {
      icase = 1;
    } else if( 0 == strcmp(argv[i], "--" ) ) {
      on_patterns = 1;
    } else if( argv[i][0] == '-' ) {
      printf("Unknown option %s\n", argv[i]);
      index_tool_usage();
    } else if( ! on_patterns ) {
      //printf("adding input %s\n", argv[i]);
      input_paths.push_back(argv[i]);
    } else {
      //printf("adding pattern %s\n", argv[i]);
      patterns.push_back(argv[i]);
    }
  }

  if( input_paths.size() == 0 ) index_tool_usage();
  if( patterns.size() == 0 ) index_tool_usage();

  if( output_fname ) {
    out = fopen(output_fname, "w");
    if( ! out ) {
      perror("Could not fopen");
      fprintf(stderr, "Could not open output filename '%s' for writing\n", output_fname);
      exit(-1);
    }
  }

  patarr = (const char**) malloc(sizeof(char*)*patterns.size());
  for( size_t i = 0; i < patterns.size(); i++ ) {
    patarr[i] = patterns[i].c_str();
  }
  npatterns = patterns.size();


  // compute the type of sequential search.
  type = RESULT_TYPE_DOCUMENTS; // the default
  if( count || match ) type = RESULT_TYPE_COUNT;
  else if( offsets ) type = RESULT_TYPE_DOC_OFFSETS;

  results = (results_t*) calloc(npatterns, sizeof(results_t));
  assert(results);
  counts = (int64_t*) calloc(npatterns, sizeof(int64_t));
  assert(counts);
  matches = (seq_search_match_keyvalue_t**) calloc(npatterns, sizeof(seq_search_match_keyvalue_t*));
  assert(matches);
  nmatches = (size_t*) calloc(npatterns, sizeof(size_t));
  assert(nmatches);

  err = init_sequential_search(&state, npatterns, patarr, icase, type, match);
  die_if_err(err);


  // Give any arguments to the its tool.
  {
    const char** args = (const char**) malloc(sizeof(char*)*input_paths.size());
    for( size_t i = 0; i < input_paths.size(); i++ ) {
      args[i] = input_paths[i].c_str();
    }

    rc = its_use_arguments(input_paths.size(), args);
    if( rc < 0 ) {
      printf("its_use_arguments returned %i\n", rc);
      die_if_err(ERR_IO_STR("its_use_arguments failed"));
    }

    free(args);
  }

  // We don't do a pass 0.
  rc = its_switch_passes();
  if( rc ) {
    printf("its_switch_passes returned %i\n", rc);
    die_if_err(ERR_IO_STR("its_switch_passes failed"));
  }

  // Go through the documents
  for( int64_t doc_num = 0; 1; doc_num++ ) {
    int64_t doc_len = 0;
    int64_t doc_info_len = 0;
    unsigned char* doc_info = NULL;
    int64_t num_doc_headers = 0;
    int64_t* doc_header_lens = NULL;
    unsigned char** doc_headers = NULL;
    unsigned char* doc_contents = NULL;
    rc = its_get_doc(doc_num,
                     &doc_len,
                     &doc_info_len,
                     &doc_info,
                     &num_doc_headers,
                     &doc_header_lens,
                     &doc_headers,
                     &doc_contents);

    if( rc == 0 ) {
      break; // all done!
    } else if ( rc == 1 ) {
      // We have a document.
      // Do something with doc_contents, doc_info_len, doc_info
      // now search each document - this could be a loop.
      err = run_sequential_search( &state, doc_len, doc_contents, doc_num );
      die_if_err(err);


      if( type != 0 ) {
        err = finish_sequential_search( &state, results, counts, matches, nmatches);
        die_if_err(err);

        // print out the results.
        for( q = 0; q < npatterns; q++ ) {
          results_reader_t reader;
          int64_t last_document, document, offset;
          long long int i;
  
          if( npatterns > 1 ) fprintf(out, "Query '%s' matched:%c", patarr[q], sep);
          i = 0;
          last_document = 0;
          err = results_reader_create(&reader, &results[q]);
          while( results_reader_next(&reader, &document, &offset) ) {
            if( i == 0 || last_document != document ) {
              last_document = document;
              if( i != 0 ) fprintf(out, "%c%s", sep, prefix);
              for( long k = 0; k < doc_info_len; k++ ) {
                fprintf(out, "%c", doc_info[k]);
              }
              if( (results[q].type & RESULT_TYPE_OFFSETS) ) {
                fprintf(out, "%c\t", sep);
              }
            }
            if( (results[q].type & RESULT_TYPE_OFFSETS) ) {
                fprintf(out, " %lli", (long long int) offset);
            }
            i++;
          }
          if( type != 0 && i != 0 ) fprintf(out, "%c", sep);

          total_matches += i;
        }

        if( matches[q] ) free(matches[q]);
      }

      // Now free what we got.
      rc = its_free_doc(doc_num, doc_len,
                        doc_info_len, doc_info,
                        num_doc_headers, doc_header_lens,
                        doc_headers,
                        doc_contents);
      if( rc < 0 ) {
        printf("its_free_doc returned %i\n", rc);
        die_if_err(ERR_IO_STR("its_free_doc failed"));
      }
    } else {
      // An error occured.
      printf("its_get_doc returned %i\n", rc);
      die_if_err(ERR_IO_STR("its_get_doc_info failed"));
    }
  }

  if( type == 0 ) {
    err = finish_sequential_search( &state, results, counts, matches, nmatches);
    die_if_err(err);
    for( q = 0; q < npatterns; q++ ) {
      if( npatterns > 1 ) fprintf(out, "Query '%s' matched:%c", patarr[q], sep);
      if( match ) {
        for( size_t z = 0; z < nmatches[q]; z++ ) {
          int64_t num = matches[q][z].value->num_matches;
          const unsigned char* data = matches[q][z].key->data;
          size_t len = matches[q][z].key->len;
  
            fprintf(out, "% 4lli [%lli, %lli] \"", (long long int) num, (long long int) (- num), (long long int) -1);
          for( size_t i = 0; i < len; i++ ) {
            int ch = data[i];
    
            if( ch == '\\' || ch == '"' ) fprintf(out, "\\%c", ch);
            else if( isprint(ch) ) fprintf(out, "%c", ch);
              else fprintf(out, "\\x%02x", ch);
          }
          fprintf(out, "\"%c", sep);
        }
      }
      total_matches += counts[q];
      if( matches[q] ) free(matches[q]);
      if( npatterns > 1 ) fprintf(out, "% 4lli total matches%c", (long long int) counts[q], sep);
    }
  }

  its_cleanup();

  for( q = 0; q < npatterns; q++ ) {
    results_destroy(& results[q]);
  }

  destroy_sequential_search(&state);

  free(counts);
  free(matches);
  free(nmatches);
  free(results);
  free(patarr);

  if( count || match ) {
    if( verbose ) printf("\n");
    fprintf(out, "% 4lli total matches%c", total_matches, sep);
  }

  if(out != stdout) fclose(out);

  return 0;
}

