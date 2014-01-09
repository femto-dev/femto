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

  femto/src/main_cc/search_tool.cc
*/

extern "C" {
#include "index.h"
#include "server.h"
#include "femto_internal.h"

#include "timing.h"
#include "results.h"
#include "buffer.h"
#include "hashmap.h"
#include "util.h"

#include "json.h"

#include <assert.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "query_planning.h"
}

#include "re2/re2.h"

void usage(char* name)
{
  printf("Usage: %s [options] <index_path> [<index_path>...] <pattern>\n", name);
  printf(" where options include:\n");
  printf(" -v or --verbose  print extra verbose output\n");
  printf(" --max_results <number> set the maximum number of results\n");
  printf(" --offsets request document offsets\n");
  printf(" --grep output matching lines like grep does\n");
  printf(" --grepdir directory of indexed files for extracting lines\n");
  printf(" --multigrep output matching lines grouped by line\n");
  printf(" --count Ask for only the number of results\n");
  printf(" --matches Show strings matching approximate search and/or regular expression\n");
  printf(" --suggest with --matches, show matches for some related queries\n");
  printf(" --suggest-starts divide the query pattern into at most this many parts while suggesting\n");
  printf(" --common-context Show most common leading and trailing context\n");
  printf(" --output <filename> output query results to file instead of stdout\n");
  printf(" --null seperate output lines with 0 bytes instead of newlines\n");
  printf(" --icase make the search case-insensitive\n");
  printf(" --json output json\n");
  printf(" --filter-results <re> filter resulting document names/info by the passed regular expression\n");
  exit(-1);
}

#define GLOM_CHAR '|'
#define MAX_GREP_LINE 1024

struct match_info {
  int64_t first;
  int64_t last;
  int cost;
  int match_len;
  alpha_t* match;
  int issuggestion;
  int index;
  int query;
};

struct len_info {
  long len;
  unsigned char* info;
  uintptr_t document_len;
};

struct grep_group {
  int n;
  struct len_info* len_infos;
};
struct grep_grouper {
  // key: line value: group
  hashmap_t line_to_docs;
  int nlines; // we're going to sort this.. list of keys.
  char** lines;
};

int alphacmp(const alpha_t* a, int alen, const alpha_t* b, int blen)
{
  int minlen = alen;
  if( minlen > blen ) minlen = blen;
  for( int i = 0; i < minlen; i++ ) {
    if( a[i] < b[i] ) return -1;
    if( a[i] > b[i] ) return 1;
  }
  // Now, if a is shorter it sorts before b.
  if( alen < blen ) return -1;
  if( alen > blen ) return 1;
  // If the are the same length and equal we return 0.
  return 0;
}
int matchcmp(const struct match_info* a, const struct match_info* b)
{
  if( a->issuggestion < b->issuggestion ) return -1;
  if( a->issuggestion > b->issuggestion ) return 1;

  if( a->match_len < b->match_len ) return 1;
  if( a->match_len > b->match_len ) return -1;

  return alphacmp(a->match, a->match_len, b->match, b->match_len);
}

int matchcmp_fn(const void* ap, const void* bp)
{
  return matchcmp((const struct match_info*) ap,
                  (const struct match_info*) bp);
}
error_t grep_group_add(struct grep_grouper* grouper, unsigned char* info, long info_len, char* line, long line_len, uintptr_t document_len)
{
  hm_entry_t entry;
  struct grep_group* group = NULL;
  error_t err;
  char* save_line;

  save_line = (char*) malloc(line_len+1);
  memcpy(save_line, line, line_len);
  save_line[line_len] = '\0';

  entry.key = save_line;
  entry.value = NULL;
  if( hashmap_retrieve(&grouper->line_to_docs, &entry) ) {
    // Found existing line
    free(save_line);
  } else {
    // New line, add entry
    entry.key = save_line;
    entry.value = calloc(1, sizeof(struct grep_group));
    err = hashmap_resize(&grouper->line_to_docs);
    if( err ) return err;
    err = hashmap_insert(&grouper->line_to_docs, &entry);
    // also add to lines.
    err = append_array(&grouper->nlines, &grouper->lines, sizeof(char*), &save_line);
    if( err ) return err;
  }
  
  // Now add this document to the group in entry.value.
  group = (struct grep_group*) entry.value;

  {
    struct len_info li;
    li.len = info_len;
    li.info = info;
    li.document_len = document_len;
    err = append_array(&group->n,&group->len_infos,sizeof(struct len_info),&li);
    if( err ) return err;
  }

  // No need to reinsert, we just changed something within the value.
  return ERR_NOERR;
}



static int
cmpstringp(const void* p1, const void *p2)
{
  return strcmp(* (char * const *) p1, * (char * const * ) p2);
}

static int
cmpleninfo(const void* p1, const void* p2)
{
  const struct len_info* a = (const struct len_info*) p1;
  const struct len_info* b = (const struct len_info*) p2;
  long minlen = a->len;
  int ret;
  if( b->len < minlen ) minlen = b->len;
  ret = memcmp(a->info, b->info, minlen);
  if( ret == 0 ) {
    if( a->len < b->len ) ret = -1;
    else if( a->len > b->len ) ret = 1;
  }
  return ret;
}

void print_document_lengths_json(FILE* out, long len, unsigned char* info, uintptr_t document_length)
{
  long k;
  fprintf(out, "%lli",(long long int) document_length);
  for( k = 0; k < len; k++ ) {
    if( info[k] == GLOM_CHAR ) {
      fprintf(out, ",%lli",(long long int) document_length);
    }
  }
}

void print_document_group_json(FILE* out, long len, unsigned char* info)
{
  long k;
  fprintf(out, "\"");
  for( k = 0; k < len; k++ ) {
    if( info[k] == GLOM_CHAR ) {
      fprintf(out, "\",\"");
    } else {
      fprint_str_json(out, 1, &info[k]);
    }
  }
  fprintf(out, "\"");
}

void complete_grep(FILE* out, char sep, struct grep_grouper* grouper, int json, int *first)
{
  int i,j;
  char after_file_sep = (sep==0)?'\0':':';
  char after_line_sep = (sep==0)?'\0':'\n';
  struct grep_group* group;
  hm_entry_t entry;
  char* line;

  // Sort the lines we have saved up.
  qsort(grouper->lines, grouper->nlines, sizeof(char*), cmpstringp);

  for( i = 0; i < grouper->nlines; i++ ) {
    line = grouper->lines[i];
    entry.key = line;
    if( hashmap_retrieve(&grouper->line_to_docs, &entry) ) {
      group = (struct grep_group*) entry.value; 
      // Sort and dedup the documents.
      group->n = sort_dedup(group->len_infos, group->n,
                            sizeof(struct len_info), cmpleninfo);
      // Now ... cary on!
      if( json ) {
        if( ! *first ) fprintf(out, ",\n   ");
        *first = 0;
        fprintf(out, "[ [");
      }
      for( j = 0; j < group->n; j++ ) {
        if( json ) {
          if( j != 0 ) fprintf(out, ", ");
          print_document_group_json(out, group->len_infos[j].len, group->len_infos[j].info);
        } else {
          if( j != 0 ) fputc(GLOM_CHAR, out);
          fprintf(out, "%.*s",
                    (int) group->len_infos[j].len,
                    (char*) group->len_infos[j].info);
        }
      }
      if( json ) {
        fprintf(out, "],\"");
        fprint_cstr_json(out, line);
        fprintf(out, "\", [");
        for( j = 0; j < group->n; j++ ) {
          if( j != 0 ) fprintf(out, ", ");
          print_document_lengths_json( out, group->len_infos[j].len, group->len_infos[j].info, group->len_infos[j].document_len );
        }
        fprintf(out, "] ]");
      } else fprintf(out, "%c%s%c", after_file_sep, line, after_line_sep);
    }
  }
}

error_t print_grep(FILE* out, unsigned char* info, long info_len, buffer_t* buf, int64_t offset, char sep, struct grep_grouper* grouper, int json, int *first, int has_offsets)
{
  // Figure out if we can extract a line from the file.
  // Look backwards for the start of a line.
  // Look forwards for the end of a line.
  int64_t line_start, line_end;
  int64_t max_line = MAX_GREP_LINE;
  int64_t i;
  int binary = 0;
  char after_file_sep = (sep==0)?'\0':':';
  char after_line_sep = (sep==0)?'\0':'\n';
  char* line;
  int nline;
  error_t err = ERR_NOERR;
  
  line_start = line_end = offset;
  nline = 0; line = NULL;

  if( buf->data && has_offsets ) {
    for( i = 1; i < max_line; i++ ) {
      if( offset - i < 0 ) break;
      if( offset - i >= (int64_t) buf->len ) break;
      if( buf->data[offset - i] == '\n' ) break;
      if( buf->data[offset - i] == '\r' ) break;
      if( buf->data[offset - i] == '\0' ) { binary = 1; break; }
      line_start = offset - i;
    }
    if( i == max_line ) binary = 1;
    for( i = 0; i < max_line; i++ ) {
      if( offset + i < 0 ) break;
      if( offset + i >= (int64_t) buf->len ) break;
      line_end = offset + i;
      if( buf->data[offset + i] == '\n' ) break;
      if( buf->data[offset + i] == '\r' ) break;
      if( buf->data[offset + i] == '\0' ) { binary = 1; break; }
    }
    if( i == max_line ) binary = 1;
  
    nline = (int) (line_end - line_start);
    line = (char*) &buf->data[line_start];
  }


  if( grouper ) {
    err = grep_group_add(grouper, info, info_len, line, nline, buf->len);
    if( err ) return err;
  } else { // not doing multigrep.
    if( json ) {
      if( ! *first ) fprintf(out, ",\n   ");
      *first = 0;
      fprintf(out, "[ [");
      print_document_group_json(out, info_len, info);
      fprintf(out, "],");
      if( binary || !buf->data || !line) {
        fprintf(out, "[]");
      } else {
        fprintf(out, "\"");
        fprint_str_json(out, nline, (unsigned char*) line);
        fprintf(out, "\"");
      }
      fprintf(out, ", %lli]", (long long int) buf->len);
    } else {
      if( binary || !buf->data || !line ) {
        fprintf(out, "%.*s%c matches%c",
                     (int) info_len, (char*) info,
                      after_file_sep, after_line_sep);
      } else {
          fprintf(out, "%.*s%c%.*s%c", (int) info_len, (char*) info,
                after_file_sep, nline, line, after_line_sep);
      }
    }
  }
  return ERR_NOERR;
}

error_t print_matches(FILE* out, 
                      femto_server_t* srv, index_locator_t loc,
                      results_t* results,
                      const char* prefix, char sep,
                      int grep, const char* grep_dir, int json, int *first,
                      RE2* filter_info)
{
  int64_t document, last_document;
  int64_t offset;
  results_reader_t reader;
  parallel_query_t ctx;
  error_t err;
  int num = 0;
  long* lens = NULL;
  unsigned char **infos = NULL;
  int i,j,k;
  char* path = NULL;
  long path_len = 0;
  long grep_dir_len = strlen(grep_dir);
  FILE* f = NULL;
  buffer_t buf;
  unsigned char* info = NULL;
  long info_len = 0;
  struct stat st;
  int rc;
  struct grep_grouper* grouper = NULL;
  int has_offsets = 0;
  int new_document = 0;
  int keep = 0;
  int first_keep = 1;

  if( grep == 2 ) {
    grouper = (struct grep_grouper*) calloc(1, sizeof(struct grep_grouper));
    if( ! grouper ) return ERR_MEM;
    err = hashmap_create(&grouper->line_to_docs, 1024, hash_string_fn, hash_string_cmp);
    die_if_err(err);
  }

  memset(&buf, 0, sizeof(buffer_t));

  err = setup_get_doc_info_results(&ctx, NULL, loc, results);
  if( err ) return err;

  err = femto_run_query(srv, (query_entry_t*) &ctx);
  if( err ) return err;

  get_doc_info_results(&ctx, &num, &lens, &infos);
  cleanup_get_doc_info_results(&ctx);

  has_offsets = is_doc_offsets(results_type(results));

  last_document = 0;
  err = results_reader_create(&reader, results);
  if( err ) return err;
  i = 0;
  j = 0;
  keep = 1;
  first_keep = 1;
  //while( j < num && results_reader_next(&reader, &document, &offset) ) {
  while( results_reader_next(&reader, &document, &offset) ) {
    new_document = 0;
    // is it a new document?
    if( i == 0 || last_document != document ) {
      new_document = 1;
      last_document = document;
      info = & (infos[j][0]);
      info_len = lens[j];

      if( filter_info ) {
        re2::StringPiece piece((const char*) info, info_len);
        if( RE2::PartialMatch(piece, *filter_info) ) {
          // OK!
          keep = 1;
        } else {
          // ignore this result
          keep = 0;
        }
      }

      if( grep ) {
        if( path_len < grep_dir_len + info_len + 1 + 1 ) { // / and \0
          path_len = grep_dir_len + info_len + 1 + 1;
          path = (char*) realloc(path, path_len);
          assert(path);
        }
        sprintf(path, "%s/%.*s", grep_dir, (int) info_len, (char*) info);
        if( f ) {
          err = munmap_buffer(&buf);
          die_if_err(err);
          fclose(f);
          f = NULL;
        }
        buf.data = NULL;
        rc = stat(path, &st);
        if( rc == 0 && S_ISREG(st.st_mode) ) {
          // OK
        } else {
          // Try truncating on GLOM_CHAR
          for( k = strlen(grep_dir)+1; path[k]; k++ ) {
            if( path[k] == GLOM_CHAR ) {
              path[k] = '\0';
              // retry stat.
              rc = stat(path, &st);
              break;
            }
          }
        }
        if( rc == 0 && S_ISREG(st.st_mode) ) {
          f = fopen(path, "r");
          if( f ) {
            err = mmap_buffer(&buf, f, 0, 0);
            if( err ) {
              fclose(f);
              f = NULL;
            }
          }
        }
      } else {
        if( keep ) {
          if( json ) {
            if( !first_keep ) fprintf(out, "] ],\n   ");
            else if( ! *first ) fprintf(out, ",\n   ");
            *first = 0;
            fprintf(out, "[ [");
            print_document_group_json(out, info_len, info);
            fprintf(out, "], [");
          } else {
            if( !first_keep ) fprintf(out,"%c%s", sep, prefix);
            fprintf(out, "%.*s", (int) info_len, info);
            if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
              fprintf(out, "%c\t", sep);
            }
          }
          first_keep = 0;
        }
      }
      j++;
    }

    if( keep ) {
      if( grep ) {
        err = print_grep(out, info, info_len, &buf, offset, sep, grouper, json, first, has_offsets);
        die_if_err(err);
      } else {
        if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
          if( json ) {
            if( ! new_document ) fprintf(out, ", ");
            fprintf(out, "%" PRIi64, offset);
          } else fprintf(out, " %" PRIi64, offset);
        }
      }
    }
    i++;
  }

  if( f ) {
    err = munmap_buffer(&buf);
    die_if_err(err);
    fclose(f);
  }
  free(path);

  if( grouper ) complete_grep(out, sep, grouper, json, first);


  if( i != 0 ) {
    if( json ) {
      if( !grep ) fprintf(out, " ] ] ");
    } else fprintf(out, "%c", sep);
  }


  for( i = 0; i < num; i++ ) {
    free(infos[i]);
  }
  free(infos);
  free(lens);

  return ERR_NOERR;
}

int main( int argc, char** argv )
{
  char** index_paths = NULL;
  int num_indexes = 0;
  char* pattern = NULL;
  char* output_fname = NULL;
  FILE* out = stdout;
  int verbose = 0;
  int64_t default_chunk_size = 1024*1024; // 1MB chunks...
  int64_t chunk_size = default_chunk_size;
  int offsets = 0;
  int grep = 0;
  const char* grepdir = ".";
  int count = 0;
  int matches = 0;
  int common_context = 0;
  char sep = '\n';
  int icase = 0;
  int json = 0;
  int suggest = 0;
  int suggest_starts = 8;
  RE2 *filter_info = NULL;

  index_paths = (char**) malloc(argc*sizeof(char*));

  {
    int i = 1;
    while( i < argc ) {
      // process options
      if( 0 == strcmp(argv[i], "-v") ||
          0 == strcmp(argv[i], "--verbose") ) {
        verbose++;
      } else if( 0 == strcmp(argv[i], "--max_results") ) {
        // set the chunk size
        i++; // pass the -c
        sscanf(argv[i], "%" SCNi64, &chunk_size);
      } else if( 0 == strcmp(argv[i], "--offsets") ) {
        offsets = 1;
      } else if( 0 == strcmp(argv[i], "--grep") ) {
        grep = 1;
        offsets = 1;
      } else if( 0 == strcmp(argv[i], "--multigrep") ) {
        grep = 2;
        offsets = 1;
      } else if( 0 == strcmp(argv[i], "--grepdir") ||
                 0 == strcmp(argv[i], "--grep-dir") ) {
        i++; // pass --grepdir
        grepdir = argv[i];
      } else if( 0 == strcmp(argv[i], "--suggeststarts") ||
                 0 == strcmp(argv[i], "--suggest-starts") ) {
        i++; // pass --suggest-starts
        suggest_starts = atoi(argv[i]);
      } else if( 0 == strcmp(argv[i], "--count") ) {
        count = 1;
      } else if( 0 == strcmp(argv[i], "--matches") ) {
        matches = 1;
        count = 1;
      } else if( 0 == strcmp(argv[i], "--suggest") ) {
        suggest = 1;
      } else if( 0 == strcmp(argv[i], "--common-context") ) {
        matches = 1;
        count = 1;
        common_context = 1;
      } else if( 0 == strcmp(argv[i], "--output") ) {
        i++; // pass --output
        output_fname = argv[i];
      } else if( 0 == strcmp(argv[i], "--null") ) {
        sep = '\0';
      } else if( 0 == strcmp(argv[i], "--json") ) {
        json = 1;
      } else if( 0 == strcmp(argv[i], "--icase") ) {
        icase = 1;
      } else if( 0 == strcmp(argv[i], "--filter-results") ) {
        i++; // pass --filter-results
        filter_info = new RE2(argv[i]);
        if( (! filter_info ) || ! filter_info->ok() ) {
          fprintf(stderr, "Could not compiler filter-results regexp\n");
          if( filter_info ) {
            fprintf(stderr, "error %s\n", filter_info->error().c_str());
          }
        }
      } else if( argv[i][0] == '-' ) {
        printf("Unknown option %s\n", argv[i]);
        usage(argv[0]);
      } else {
        index_paths[num_indexes++] = argv[i];
      }

      i++;
    }
  }

  if( json ) {
    sep = '\n';
  }

  if( num_indexes < 2 ) usage(argv[0]);

  // pattern takes the last index path.
  num_indexes--;
  pattern = index_paths[num_indexes];

  if( output_fname ) {
    out = fopen(output_fname, "w");
    if( ! out ) {
      perror("Could not fopen");
      fprintf(stderr, "Could not open output filename '%s' for writing\n", output_fname);
      exit(-1);
    }
  }

  {
    error_t err;
    index_locator_t *locs;
    femto_request_t *reqs;
    femto_server_t srv;
    struct ast_node* ast;
    int64_t total_matches = 0;
    struct ast_node** unbooled = NULL;
    int nunbooled = 0;
    int req = 0;
    int nreq = 0;
    char** parts = NULL;
    char* echo_query = NULL;
    int first_match = 1;
    string_t string = {0,NULL};
    struct match_info* matcheis = NULL;
    int nmatcheis = 0;
    int next;
    int in_suggestions = 0;
    int last_suggestion_length = 0;

    result_type_t type = (offsets)?
                           (RESULT_TYPE_DOC_OFFSETS):
                             (RESULT_TYPE_DOCUMENTS);

    ast = parse_string(strlen(pattern), pattern);
    if( ! ast ) {
      fprintf(stderr, "Could not parse pattern\n");
      exit(-1);
    }
   
    //Remove useless extra tokens from the Regular Expression
    streamline_query(ast);

    //Convert to string queries when there are no regexp alternatives
    err = simplify_query(&ast);
    die_if_err(err);

    // Now, if we have a string query, extract the pattern because
    //  we're going to use it with the splits.
    if( ast->type == AST_NODE_STRING ) {
      struct string_node* s = (struct string_node*) ast;
      string = dup_string(s->string);
      if( verbose ) {
        printf("Extracted pattern: ");
        fprint_alpha(out, string.len, string.chars);
        printf("\n");
      }
    }

    if( icase ) {
      icase_ast(&ast);
    }

    if( count || matches ) {
      if( !string.chars ) {
        err = unbool_ast(ast, &unbooled, &nunbooled);
        die_if_err(err);
        assert(nunbooled > 0);
        assert(unbooled != NULL);
        // We don't suggest for Boolean or Regexp queries.
        suggest = 0;
      } else if( suggest ) {
        int laststart = 0;
        if( suggest_starts < 1 ) suggest_starts = 1;
        // we have a string... maybe we want to put a bunch
        //  of starting points into unbooled?
        for( int i = 0; i < suggest_starts; i++ ) {
          // these are offsets from the right.
          int mystart = i * string.len / suggest_starts;
          // convert them to offsets from the left.
          mystart = string.len - mystart;
          // make sure that they are in bounds.
          if( mystart > string.len ) mystart = string.len;
          if( mystart <= 0 ) mystart = 1;
          if( mystart != laststart && 0 <= mystart && mystart <= string.len ) {
            string_t str;
            laststart = mystart;
            str.len = mystart;
            str.chars = string.chars;
            if( verbose ) {
              printf("Suggest pattern %i %i: ", i, mystart);
              fprint_alpha(out, str.len, str.chars);
              printf("\n");
            }

            struct string_node* n = string_node_new(dup_string(str));
            // icase again.
            if( icase ) {
              icase_ast((struct ast_node**) &n);
            }
            err = append_array(&nunbooled, &unbooled, sizeof(struct ast_node*), &n);
            die_if_err(err);
          }
        }
      }
    }

    if( ! unbooled ) {
      err = append_array(&nunbooled, &unbooled, sizeof(struct ast_node*), &ast);
      die_if_err(err);
    }

    // Bug fix: Boolean searches not working correctly
    // with smaller chunk sizes.
    if( ast_is_bool(ast) ) {
      chunk_size = default_chunk_size;
    }

    nreq = num_indexes*nunbooled;

    reqs = (femto_request_t*) calloc(nreq,sizeof(femto_request_t));
    locs = (index_locator_t*) calloc(num_indexes,sizeof(index_locator_t));
    parts = (char**) calloc(nunbooled,sizeof(char*));
    assert( reqs && locs && parts );

    for( int j = 0; j < nunbooled; j++ ) {
      parts[j] = ast_to_string(unbooled[j], 0, 1);
    }
    echo_query = ast_to_string(ast, 0, 1);

    if( verbose ) {
      printf("Query pattern is: %s\n", echo_query);
      print_ast_node(stdout, ast, 0);
      if( verbose > 1 ) {
        for( int j = 0; j < nunbooled; j++ ) {
          printf("Unbooled %i:\n", j);
          print_ast_node(stdout, unbooled[j], 0);
        }
      }
      printf("Chunk size is %lli\n", (long long int) chunk_size);
    }

    // start the server.
    err = femto_start_server_err(&srv, verbose);
    die_if_err( err );

    // Get the locators for the indexes we're using.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_loc_for_path_err(&srv, index_paths[i], &locs[i]);
      die_if_err( err );
    }

    // Create the requests.
    for( int i = 0; i < num_indexes; i++ ) {
      assert(req < nreq);
      for( int j = 0; j < nunbooled; j++ ) {
        if( matches || count ) {
          process_entry_t* q = NULL;
          err = create_generic_ast_count_query(&q, NULL, locs[i], unbooled[j]);
          die_if_err(err);
          err = femto_setup_request_err(&reqs[req], (query_entry_t*) q, 1);
          die_if_err(err);
        } else {
          results_query_t* q = NULL;
          err = create_generic_ast_query(&q, NULL, locs[i], chunk_size, type, ast);
          die_if_err(err);
          err = femto_setup_request_err(&reqs[req], (query_entry_t*) q, 1);
          die_if_err(err);
        }
        req++;
      }
    }

    if( verbose ) start_clock();

    assert(req == nreq);

    // Run queries one index at a time.
    for( int i = 0; i < num_indexes; i++ ) {
      // Start all of the queries.
      err = femto_begin_requests_err(&srv, &reqs[i*nunbooled], nunbooled);
      die_if_err(err);
      // Wait for all of the queries to finish.
      for( int j = 0; j < nunbooled; j++ ) {
        err = femto_wait_request_err(&srv, &reqs[i*nunbooled+j]);
        die_if_err(err);
      }
   }
   /* ... if we wanted to query all indexes at once...
    for( int i = 0; i < nreq; i++ ) {
      err = femto_begin_request_err(&srv, &reqs[i]);
      die_if_err(err);
    }
    for( int i = 0; i < nreq; i++ ) {
      err = femto_wait_request_err(&srv, &reqs[i]);
      die_if_err(err);
    }
   */


    if( verbose ) stop_clock();

    if( json ) {
      fprintf(out, "{\n");
      fprintf(out, " \"pattern\":\"");
      fprint_cstr_json(out, echo_query);
      fprintf(out, "\",\n \"results\":[\n   ");
    }

    first_match = 1;

    // Accumulate into matcheis any matches so we can
    // have a simpler print loop for them.
    if( count || matches ) {
      struct match_info m;
      for( int i = 0; i < nreq; i++ ) {
        query_entry_t* q = reqs[i].query;
        int index = i / nunbooled;
        int query = i % nunbooled;

        if( q->type == QUERY_TYPE_REGEXP ) {
          regexp_query_t* rq = (regexp_query_t*) q;
          // Go through the results...
          for( int j = 0; j < rq->results.num_results; j++ ) {
            regexp_result_t result = rq->results.results[j];
            if( result.last >= result.first ) {
              memset(&m, 0, sizeof(struct match_info));
              m.first = result.first;
              m.last = result.last;
              m.cost = result.cost;
              m.match_len = result.match_len;
              m.match = result.match;
              m.issuggestion = (suggest && query != 0);
              m.index = index;
              m.query = query;
              //printf(" regexp [%" PRIi64 ",%" PRIi64 "]\n", m.first, m.last);
              err = append_array(&nmatcheis, &matcheis, sizeof(struct match_info), &m);
              die_if_err(err);
            }
          }
          // ... and the nonresults... (suggestions)
          if( suggest ) {
            for( int j = 0; j < rq->nonresults.num_results; j++ ) {
              regexp_result_t result = rq->nonresults.results[j];
              if( result.last >= result.first ) {
                memset(&m, 0, sizeof(struct match_info));
                m.first = result.first;
                m.last = result.last;
                m.cost = result.cost;
                m.match_len = result.match_len;
                m.match = result.match;
                m.issuggestion = 1;
                m.index = index;
                m.query = query;
                //printf(" re suggest [%" PRIi64 ",%" PRIi64 "]\n", m.first, m.last);
                err = append_array(&nmatcheis, &matcheis, sizeof(struct match_info), &m);
                die_if_err(err);
              }
            }
          }

        } else if( q->type == QUERY_TYPE_STRING ) {
          string_query_t* sq = (string_query_t*) q;
          if( sq->last >= sq->first ) {
            memset(&m, 0, sizeof(struct match_info));
            m.first = sq->first;
            m.last = sq->last;
            m.cost = 0;
            m.match_len = sq->plen;
            m.match = sq->pat;
            m.issuggestion = (suggest && query != 0);
            m.index = index;
            m.query = query;
            err = append_array(&nmatcheis, &matcheis, sizeof(struct match_info), &m);
            //printf(" string [%" PRIi64 ",%" PRIi64 "]\n", m.first, m.last);
            die_if_err(err);
          }
          if( suggest &&
              sq->prev_last >= sq->prev_first &&
              sq->prev_len >= 0 && sq->prev_len <= sq->plen ) {
            memset(&m, 0, sizeof(struct match_info));
            m.first = sq->prev_first;
            m.last = sq->prev_last;
            m.cost = 0;
            m.match_len = sq->prev_len;
            m.match = &sq->pat[sq->plen - sq->prev_len];
            m.issuggestion = 1;
            m.index = index;
            m.query = query;
            //printf(" string suggest [%" PRIi64 ",%" PRIi64 "]\n", m.first, m.last);
            err = append_array(&nmatcheis, &matcheis, sizeof(struct match_info), &m);
            die_if_err(err);
          }
        }
      }

      if( suggest ) {
        struct match_info non_suggestion;
        memset(&non_suggestion, 0,sizeof(struct match_info));
        memset(&m, 0,sizeof(struct match_info));
        // Set to ignore suggestions that:
        //  - are a prefix or a suffix of the matched pattern
        //  - have the same number of matches as a matched pattern
        for( int i = 0; i < nmatcheis; i++ ) {
          m = matcheis[i];
          if( !m.issuggestion ) {
            non_suggestion = m;
          } else {
            int64_t m_count = m.last - m.first + 1;
            int64_t n_count = non_suggestion.last - non_suggestion.first + 1;
            /*int isprefix = alpha_begins_with(string.chars, string.len,
                                             m.match, m.match_len);
            int issuffix = alpha_ends_with(string.chars, string.len,
                                            m.match, m.match_len);*/
            if( m.match_len < non_suggestion.match_len &&
                n_count == m_count ) {
              // Don't report suggestions that are just a
              //  substring of something we already know matched.
              matcheis[i].issuggestion = -1;
            }
          }
        }
      }
      // Sort the matches by:
      //   -- issuggestion
      //   -- pattern length
      //   -- pattern
      qsort(matcheis, nmatcheis, sizeof(struct match_info), matchcmp_fn);

      memset(&m, 0, sizeof(struct match_info));
      
      in_suggestions = 0;
      total_matches = 0;
      next = nmatcheis;
      for( int i = 0; i < nmatcheis; i = next ) {
        int64_t num_matches = 0;

        // skip suggestions we marked to skip above.
        if( matcheis[i].issuggestion < 0 ) {
          next = i + 1;
          continue;
        }

        // Figure out the next match that has a different pattern.
        // and get the total number of matches for this pattern.
        for( next = i; next < nmatcheis; next++ ) {
          m = matcheis[next];
          if( m.last >= m.first ) {
            if( 0 == matchcmp(&matcheis[i], &matcheis[next]) ) {
              num_matches += m.last - m.first + 1;
              if( verbose ) {
                fprintf(out, "Results from %s\n", index_paths[m.index]);
                fprintf(out, "For pattern %s\n", parts[m.query]);
                
                if( m.issuggestion ) fputc('*', out);
                fprintf(out, "% 4" PRIi64  " [%" PRIi64 ",%" PRIi64 "] \"", m.last - m.first + 1, m.first, m.last);
                fprint_alpha(out, m.match_len, m.match);
                fprintf(out, "\"%c", sep);
              }
            } else {
              // Different pattern.
              break;
            }
          }
        }

        if( num_matches == 0 ) continue;

        m = matcheis[i];

        if( m.issuggestion > 0 ) {
          if (!in_suggestions ) {
            in_suggestions = 1;
            if( json ) {
              first_match = 1;
              fprintf(out, " ],\n \"suggestions\":[\n   ");
            }
            last_suggestion_length = m.match_len;
          }
          if( m.match_len != last_suggestion_length ) {
            break;
          }
        }

        // Now print out a row.
        if( json ) {
          if( !first_match ) fprintf(out, ",\n   ");
          fprintf(out, "[\"");
          fprint_alpha_json(out, m.match_len, m.match);
          fprintf(out, "\"");
          fprintf(out, ", %" PRIi64 "]", num_matches);
        } else {
          if( m.issuggestion ) fputc('*', out);
          fprintf(out, "% 4" PRIi64 " \"", num_matches);
          fprint_alpha(out, m.match_len, m.match);
          fprintf(out, "\"%c", sep);
        }
        first_match = 0;
        if( ! m.issuggestion ) total_matches += num_matches;
      }
    } else {
      for( int i = 0; i < nreq; i++ ) {
        query_entry_t* q = reqs[i].query;
        //int index = i / nunbooled;
        //int query = i % nunbooled;
        results_query_t* rq = (results_query_t*) q;
        print_matches(out, &srv, locs[i], &rq->results, "", sep,
                      grep, grepdir, json, &first_match, filter_info);
      }
    }

    if( json ) fprintf(out, " ]");

    for( int i = 0; i < nreq; i++ ) {
      femto_cleanup_request( & reqs[i] );
    }


    if( count || matches ) {
      if( verbose && !json ) printf("\n");
      if( json ) fprintf(out, ",\n \"total\":%" PRIi64, total_matches);
      else fprintf(out, "% 4" PRIi64 " total matches%c", total_matches, sep);
    }
    if( json ) fprintf(out, "\n}\n");

    free_ast_node(ast);

    //printf("Block requests: %" PRIi64 " faults %" PRIi64 "\n",
    //       sst->stats.block_requests, sst->stats.block_faults);
    femto_stop_server(&srv);

    free(locs);
    free(reqs);
    if( matches || count ) free(unbooled);
    for( int j = 0; j < nunbooled; j++ ) {
      free(parts[j]);
    }
    free(parts);
    free(echo_query);
  }

  if( verbose && ! json) {
    print_timings("indexes queried", num_indexes);
  }

  if(out != stdout) fclose(out);

  free(index_paths);

  return 0;
}

