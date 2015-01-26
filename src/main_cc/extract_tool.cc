/*
  (*) 2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main_cc/extract_tool.cc
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
#include <vector>
#include <set>

void usage(char* name)
{
  printf("Usage: %s [options] <index_path>\n", name);
  printf(" extract finds the most common substrings in an index using breadth-first-search\n");
  printf(" where options include:\n");
  printf(" --filter-names <re> filter resulting document names/info by the passed regular expression\n");
  printf(" --document-names <filename> only extract the named documents\n");
  printf("                  <filename> should contain 1 name per line\n");
  printf("                  <filename> can be - for stdin\n");
  printf(" --document-names0 <filename> only extract the named documents\n");
  printf("                   <filename> should contain names separated by the zero byte\n");
  printf("                   <filename> can be - for stdin\n");
  printf(" --document-ids <filename> only extract the documents with the ids in <filename>, numeric IDs, one per line, first document is 1\n");
  printf(" --outdir <path> store all extracted documents within outdir (default is the current directory)\n");
  printf(" --list only list matching documents (do not extract)\n");
  exit(-1);
}

typedef enum {
  DO_FILTER_RE,
  DO_USE_NAMES,
  DO_USE_NAMES0,
  DO_USE_IDS,
} filter_type_t;

struct my_filters {
  filter_type_t type;
  const char* arg;
  my_filters(filter_type_t type, const char* arg) : type(type), arg(arg) { }
};

struct extracted_document {
  int64_t doc_id;
  const unsigned char* doc_info;
  int64_t doc_info_len;
  int64_t doc_len;
  extracted_document() : doc_id(-1), doc_info(NULL), doc_info_len(0), doc_len(0) { }
};

// Extract the documents passed in
// free all memory associated with the documents passed in
// clear out the vector of work passed in.
void do_extract(std::vector<extracted_document> & docs,
                index_locator_t loc,
                femto_server_t srv,
                const char* out_dir)
{

  error_t err;
  parallel_query_t pq;
  long num = docs.size(); 

  err = setup_parallel_query(&pq, NULL, loc, sizeof(extract_document_query_t), num);
  die_if_err( err );

  for( long i = 0; i < num; i++ ) {
    err = setup_extract_document_query((extract_document_query_t*) ith_query(&pq, i), &pq.proc, loc, docs[i].doc_id);
    die_if_err( err );
  }

  err = femto_run_query(&srv, (query_entry_t*) &pq);
  die_if_err( err );

  for( long i = 0; i < num; i++ ) {
    extract_document_query_t* edq = (extract_document_query_t*) ith_query(&pq,i);
    if( edq->proc.entry.err_code ) {
      die_if_err( ERR_MAKE( (err_code_t) edq->proc.entry.err_code ) );
    }

    // Write this document to a file.
    std::string path(out_dir);
    std::string doc_info((const char*) docs[i].doc_info, docs[i].doc_info_len);
    path += "/";
    path += doc_info;

    FILE* f = fopen(path.c_str(), "w");

    if( ! f ) {
      perror("Could not fopen");
      fprintf(stderr, "Could not open output filename '%s' for writing\n", path.c_str());
      exit(-1);
    }
 
    int64_t len = docs[i].doc_len;
    const alpha_t* contents = edq->content;
    len--; // omit EOF characters
    for( int64_t j = 0; j < len; j++ ) {
      int rc = fputc(contents[j] - CHARACTER_OFFSET, f);
      if( rc == EOF ) {
        perror("Could not fputc");
        fprintf(stderr, "Could not write\n");
      }
    }
    fclose(f);


    cleanup_extract_document_query(edq);

    free((void*)docs[i].doc_info);
    docs[i].doc_info = NULL;
  }

  docs.clear();
}

 
int main( int argc, char** argv )
{
  char* index_path = NULL;
  const char* out_dir = "./";
  int verbose = 0;
  std::vector<struct my_filters> filters;
  size_t extract_in_parallel = 100;
  int list = 0;

  {
    int i = 1;
    while( i < argc ) {
      // process options
      if( 0 == strcmp(argv[i], "-v") ||
          0 == strcmp(argv[i], "--verbose") ) {
        verbose++;
      } else if( 0 == strcmp(argv[i], "-l") ||
                 0 == strcmp(argv[i], "--list") ) {
        list++;
      } else if( 0 == strcmp(argv[i], "--filter-names") ) {
        i++;
        filters.push_back(my_filters(DO_FILTER_RE,argv[i]));
      } else if( 0 == strcmp(argv[i], "--document-names") ) {
        i++;
        filters.push_back(my_filters(DO_USE_NAMES,argv[i]));
      } else if( 0 == strcmp(argv[i], "--document-names0") ) {
        i++;
        filters.push_back(my_filters(DO_USE_NAMES0,argv[i]));
      } else if( 0 == strcmp(argv[i], "--document-ids") ) {
        i++;
        filters.push_back(my_filters(DO_USE_IDS,argv[i]));
      } else if( 0 == strcmp(argv[i], "--outdir") ) {
        i++;
        out_dir = argv[i];
      } else if( argv[i][0] == '-' ) {
        printf("Unknown option %s\n", argv[i]);
        usage(argv[0]);
      } else {
        if( index_path ) usage(argv[0]);
        index_path = argv[i];
      }
      i++;
    }
  }

  if( out_dir == NULL ) usage(argv[0]);
  if( index_path == NULL ) usage(argv[0]);

  // figure out which re filters to apply
  std::vector<RE2*> extract_re;
  // figure out which document names we are going to extract.
  std::set<std::string> extract_names;
  // figure out which document IDs we are going to extract.
  std::set<int64_t> extract_ids;

  for( size_t i = 0; i < filters.size(); i++ ) {
    filter_type_t type = filters[i].type;
    const char* arg = filters[i].arg;
    switch (type) {
      case DO_FILTER_RE:
        extract_re.push_back(new RE2(arg));
        break;
      case DO_USE_NAMES:
      case DO_USE_NAMES0:
        {
          int sep = 0;
          FILE* f = fopen(arg, "r");
          std::string line;

          if( type == DO_USE_NAMES ) sep = '\n';

          if( ! f ) {
            perror("Could not fopen");
            fprintf(stderr, "Could not open input filename '%s'\n", arg);
            exit(-1);
          }

          while( 1 ) {
            int ch = fgetc(f);
            if( ch == sep || ch == EOF ) {
              if( !line.empty() ) extract_names.insert(line);
              line.clear();
              if( ch == EOF ) break;
            } else {
              line.push_back(ch);
            }
          }
          fclose(f);
        }
        break;
      case DO_USE_IDS:
        {
          FILE* f = fopen(arg, "r");
          uint64_t num;
          int got;

          if( ! f ) {
            perror("Could not fopen");
            fprintf(stderr, "Could not open input filename '%s'\n", arg);
            exit(-1);
          }

          while( 1 ) {
            num = 0;
            got = fscanf(f, " %" SCNu64, &num);
            if( got == 1 ) extract_ids.insert(num);
            else if( got == EOF ) break;
            else {
              perror("Could not fscanf");
              fprintf(stderr, "Error in fscanf");
              exit(-1);
            }
          }
          fclose(f);
        }
        break;
    }
  }

  {
    error_t err;
    index_locator_t loc;
    femto_server_t srv;
    int64_t num_docs;
    int64_t i;

    // open up the index.
    err = femto_start_server_err(&srv, verbose);
    die_if_err( err );

    err = femto_loc_for_path_err(&srv, index_path, &loc);
    die_if_err(err);

    // step 1: get the number of documents
    {
      header_loc_query_t q;
      setup_header_loc_query(&q, loc, 0, NULL, HDR_LOC_REQUEST_NUM_DOCS, 0, 0);

      err = femto_run_query(&srv, (query_entry_t*) &q);
      die_if_err(err);

      if( q.leaf.entry.err_code ) {
        die_if_err( ERR_MAKE( (err_code_t) q.leaf.entry.err_code ) );
      }
      num_docs = q.r.doc_len;
      cleanup_header_loc_query(&q);
    }

    // Now go throug the list of document infos for all documents
    // in order to get only document IDs.
    if( !(extract_re.empty() && extract_names.empty() ) ) {
      // fill in extract_ids... for matching documents...

      // step 2: get the doc infos for each document
      for( i = 0; i < num_docs; i++ ) {
        header_loc_query_t hq;
        setup_header_loc_query(&hq, loc, 0, NULL, HDR_LOC_REQUEST_DOC_INFO, 0, i);

        err = femto_run_query(&srv, (query_entry_t*) &hq);
        die_if_err(err);

        if( hq.leaf.entry.err_code ) {
          die_if_err( ERR_MAKE( (err_code_t) hq.leaf.entry.err_code ) );
        }

        int keep = 0;
        const unsigned char* doc_info = hq.r.doc_info;
        int64_t doc_info_len = hq.r.doc_info_len;
        re2::StringPiece piece((const char*) doc_info, doc_info_len);
        std::string str((const char*) doc_info, doc_info_len);
        // Check the regular expressions
        for( size_t j = 0; j < extract_re.size(); j++ ) {
          RE2* filter = extract_re[j];
          if( RE2::PartialMatch(piece, *filter) ) {
            // OK!
            keep = 1;
          }
        }

        // Check the document set
        if( extract_names.count(str) ) {
          keep = 1;
        }

        cleanup_header_loc_query(&hq);

        // If we decided to keep it, add it to the list of
        // document numbers
        if( keep ) {
          extract_ids.insert(i);
        }
      }
    } else if( extract_ids.empty() ) {
      // Add all document numbers to extract_ids.
      for( i = 0; i < num_docs; i++ ) {
        extract_ids.insert(i);
      }
    }
 
    std::vector<extracted_document> extracting;

    // Now extract the documents in extract_ids.
    for(std::set<int64_t>::iterator it = extract_ids.begin();
        it != extract_ids.end();
        ++it) {

      int64_t doc_id = *it;
      header_loc_query_t hq;
      setup_header_loc_query(&hq, loc, 0, NULL, (header_loc_request_type_t)(HDR_LOC_REQUEST_DOC_INFO|HDR_LOC_REQUEST_DOC_LEN), 0, doc_id);

      err = femto_run_query(&srv, (query_entry_t*) &hq);
      die_if_err(err);

      if( hq.leaf.entry.err_code ) {
        die_if_err( ERR_MAKE( (err_code_t) hq.leaf.entry.err_code ) );
      }
      const unsigned char* doc_info = hq.r.doc_info;
      int64_t doc_info_len = hq.r.doc_info_len;
      int64_t doc_len = hq.r.doc_len;


      // Steal some allocated memory frm hq
      hq.r.doc_info = NULL;
      cleanup_header_loc_query(&hq);

      if( verbose ) {
        printf(" %"PRIi64" bytes in document %"PRIi64":%"PRIi64": ", doc_len, doc_id, doc_info_len);
      }
      if( verbose || list ) {
        for( int64_t j = 0; j < doc_info_len; j++ ) {
          putchar(doc_info[j]);
        }
        printf("\n");
      }

      extracted_document doc;
      doc.doc_id = doc_id;
      doc.doc_info = doc_info;
      doc.doc_info_len = doc_info_len;
      doc.doc_len = doc_len;
      extracting.push_back(doc);

      // We could sort these extracted documents by size
      // or do some other load balancing tactic. s it is,
      // we'll just extract a bunch at once.
      if( extracting.size() >= extract_in_parallel ) {
        do_extract(extracting, loc, srv, out_dir);
      }
    }

    if( !extracting.empty() ) do_extract(extracting, loc, srv, out_dir);

    femto_stop_server(&srv);
  }

  for( size_t i = 0; i < extract_re.size(); i++ ) {
    delete extract_re[i];
  }

  return 0;
}
