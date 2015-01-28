/*
  (*) 2014-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/similar_tool.c
*/


/*
FINDING SIMILAR DOCUMENTS IN A FEMTO INDEX  --------------------

This algorithm provides FEMTO with the ability to find files in an index that
are similar to a provided file (which is generally expected not to be in the
index). This software can compute three kinds of output:
 1) A similarity score with the index
 2) A similarity score with each similar document
 3) A list of byte sequences shared by the provided file and any files in the
index but that do not occur in any "--not" indexes provided as containing
uninteresting material.

This software is useful in literary authorship analysis, plagarism detection,
and more generally as a strategy to simultaneously check for the presence of a
large number search sequences in a provided file. The search sequences are
represented by the index. The "--not" indexes can be used like a stop list in
information retrieval to remove results that are uninteresting. For example,
in the plagarism detection case, one would create several different indices
representing different combinations of works that could be plagarized.  Then,
to detect if a work has significantly copied one of the source works, one
would look for a high similarity score. The "--not" indexes would be used in
this case to filter out of consideration certain words or phrases that are
legitimately common to many works (including common words or phrases or
possibly wording from the classroom assignment).

The similarity scores are based upon the counts of common byte sequences which
are computed in a manner described below and filtered out by some set of
indexes containing uninteresting data.  The similarity of material in an index
to the provided document is computed as follows. The total score adds up the
number of positions in the input accounted for by a common byte sequence above
a particular minimum size:
 total score = sum over common byte sequences of
          sequence length *
              min(number of occurences in the provided document,
                  number of occurences in the indexed document)

The algorithm works as follows. First, it runs the 'step' function which
starts with every offset in the provided file and develops common sequences
one byte at a time in a breadth-first but backwards manner.  A data structure
keeps track of each unique common sequence (e.g.  'abc'), the offsets that
sequence occurs in the provided file, and the first and last row in the index
that matches that sequence. 

We start out with 1-byte unique common sequences for all unique bytes that
occur in the provided file. Then, each time through the 'step' loop, these
sequences are extended backwards by 1 byte if the extended sequence appears
both in the provided document and in the index. In this manner, the algorithm
finds the longest sequences common to the provided file and to the index,
along with the offsets of those sequences in the provided file and the number
of times each sequence occurs in the index.

Next, the algorithm removes common sequences that are completely contained in
other common sequences since they are not interesting to report. For example,
the algorithm removes 'bc' if 'abc' or 'bcd' are found to be common between
the input document and the index.

Next, the algorithm removes any common sequences that appear in the "--not"
indexes which contain uninteresting data.

Finally, the algorithm shows the results and computes per-index and
per-document similarity scores in the manner described above.
*/

#include "index.h"
#include "server.h"
#include "femto_internal.h"

#include "hashmap.h"
#include "buffer.h"
#include "timing.h"

#include <stdio.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <assert.h>

const int dbg = 0;

struct row_range {
  int64_t first;
  int64_t last;
};

typedef uint32_t my_offset_t;
//typedef uintptr_t my_offset_t;

struct common_s {
  my_offset_t common; // T[offset-common+1...offset] is string
  my_offset_t noffsets;
  my_offset_t offsets_start_in_S; // Offsets in original data.
  int64_t first;
  int64_t last;
};

struct report_s {
  my_offset_t offset; // T[offset..offset+len-1] is string
  int group;
};

struct group_s {
  int index;
  my_offset_t min_offset; // T[offset..offset+len-1] is string
  my_offset_t len;
  my_offset_t n_offsets;
  int64_t n_index_offsets;
};

struct group_map_s {
  int id;
  int new_id;
};

buffer_t mapped;
my_offset_t *S;
my_offset_t *nextS;
struct row_range rows_by_char[256];
my_offset_t input_length;

int n_reports;
struct report_s *reports;
int n_groups;
struct group_s *groups;

int min_match_length = 8;
int max_match_length = INT_MAX;
int max_got_match_length = 0;
int stop_same_match_length = 12;
char** not_index_paths = NULL;
int num_not_indexes = 0;
int max_offsets = 100;

int verbose = 0;

femto_server_t srv;

#define QUERY_LENGTH 4
#define MAX_PATTERN_LENGTH (32*1024)

static inline
int getbyte_at(my_offset_t data_index)
{
  intptr_t offset = data_index;
  if( offset < 0 || offset >= mapped.len ) return -1;
  else return mapped.data[offset];
}


static inline
int getbyte(const struct common_s* common, int i)
{
  intptr_t offset = S[common->offsets_start_in_S + i] - common->common;
  if( offset < 0 || offset >= mapped.len ) return -1;
  else return mapped.data[offset];
}

static inline
void getbuf(unsigned char buf[QUERY_LENGTH], my_offset_t offset)
{
  int i;
  for( i = 0; i < QUERY_LENGTH; i++ ) {
    buf[i] = (offset+i<mapped.len)?(mapped.data[offset+i]):(0);
  }
}

typedef struct score_s {
  long unique_seqs_common;
  long total_len_common;
  long min_common_length;
  long max_common_length;
  int64_t max_other_document_length;
  double score;
} score_t;

static inline
void init_score(score_t *accum)
{
  struct score_s zero_score = {0};
  *accum = zero_score;
  accum->min_common_length = max_match_length;
  accum->max_common_length = min_match_length;
}
static inline
void accumulate_score(score_t *accum,
                      my_offset_t pattern_length,
                      my_offset_t noccurs_here,
                      int64_t noccurs_there,
                      int64_t doc_len_there)
{
  my_offset_t min_noccurs;
  min_noccurs = (noccurs_here<noccurs_there)?noccurs_here:noccurs_there;

  accum->unique_seqs_common++;
  accum->total_len_common += pattern_length * min_noccurs;
  if( pattern_length < accum->min_common_length )
    accum->min_common_length = pattern_length;
  if( pattern_length > accum->max_common_length )
    accum->max_common_length = pattern_length;

  if( doc_len_there > accum->max_other_document_length )
    accum->max_other_document_length = doc_len_there;
  
  // Score is used for sorting the documents.
  accum->score += pattern_length * min_noccurs;

  /*
  if( doc_len_there > 0 ) {
    printf("adding to score: here %i there %i len %i min %i\n",
        (int) noccurs_here,
        (int) noccurs_there,
        (int) pattern_length,
        (int) min_noccurs);
    printf("score %lf total_len %i\n",
           accum->score,
           accum->total_len_common);
  }*/
}

static inline
double compute_score(score_t *accum)
{
  int64_t use_len = input_length;
  if( accum->max_other_document_length > 0 &&
      accum->max_other_document_length < use_len ) {
    use_len = accum->max_other_document_length;
  }
  //printf("%i total len common uselen %i\n", accum->total_len_common, use_len);

  return (double) accum->total_len_common / (double) use_len;
}

void print_score(const char* msg, score_t* accum)
{
  double sc = compute_score(accum);
  printf("%s %lf : %li <= len <= %li : %li sequences : %li bytes\n",
         msg, 100.0*sc,
         accum->min_common_length, accum->max_common_length,
         accum->unique_seqs_common,
         accum->total_len_common);
}

int compare_report(const void* av, const void* bv)
{
  struct report_s* a = (struct report_s*) av;
  struct report_s* b = (struct report_s*) bv;
  struct group_s* ag = &groups[a->group];
  struct group_s* bg = &groups[b->group];

  if( a->group < 0 || a->group >= n_groups || b->group < 0 || b->group >= n_groups ) {
    if( a->group < b->group ) return -1;
    if( a->group > b->group ) return 1;
    return 0;
  }

  if( ag->len < bg->len ) return 1;
  if( ag->len > bg->len ) return -1;
  if( a->group < b->group ) return -1;
  if( a->group > b->group ) return 1;
  if( a->offset < b->offset ) return -1;
  if( a->offset > b->offset ) return 1;
  return 0;
}
int rev_compare_report(const void* av, const void* bv)
{
  struct report_s* a = (struct report_s*) av;
  struct report_s* b = (struct report_s*) bv;
  struct group_s* ag = &groups[a->group];
  struct group_s* bg = &groups[b->group];
  my_offset_t a_end;
  my_offset_t b_end;

  if( a->group < 0 || a->group >= n_groups || b->group < 0 || b->group >= n_groups ) {
    if( a->group < b->group ) return -1;
    if( a->group > b->group ) return 1;
    return 0;
  }

  a_end = a->offset + ag->len - 1;
  b_end = b->offset + bg->len - 1;
  if( a_end < b_end ) return 1;
  if( a_end > b_end ) return -1;
  if( a->offset < b->offset ) return 1;
  if( a->offset > b->offset ) return -1;
  return 0;
}


int compare_group_id_text(const void* av, const void* bv)
{
  struct group_map_s* am = (struct group_map_s*) av;
  struct group_map_s* bm = (struct group_map_s*) bv;
  int ai = am->new_id;
  int bi = bm->new_id;
  struct group_s* a;
  struct group_s* b;
  my_offset_t alen;
  my_offset_t blen;
  unsigned char* aptr;
  unsigned char* bptr;
  my_offset_t minlen;
  int cmp;

  if( ai == -1 || ai >= n_groups || bi == -1 || bi >= n_groups ) {
    if( ai < bi ) return -1;
    if( ai > bi ) return 1;
    return 0;
  }

  a = &groups[ai];
  b = &groups[bi];

  if( a->index < b->index ) return -1;
  if( a->index > b->index ) return 1;

  alen = a->len;
  blen = b->len;
  aptr = &mapped.data[a->min_offset];
  bptr = &mapped.data[b->min_offset];
  minlen = (alen<blen)?alen:blen;

  cmp = memcmp(aptr, bptr, minlen);
  if( cmp != 0 ) return cmp;
  // otherwise the patterns are either identical
  // or one is a prefix of the other.
  if( alen < blen ) return -1;
  if( alen > blen ) return 1;
  return 0;
}

int compare_group_id(const void* av, const void* bv)
{
  struct group_map_s* am = (struct group_map_s*) av;
  struct group_map_s* bm = (struct group_map_s*) bv;
  int ai = am->id;
  int bi = bm->id;
  
  
  if( ai < bi ) return -1;
  if( ai > bi ) return 1;
  return 0;
}
 

struct query_slot {
  alpha_t* query;
  hash_t slot;
};

int compare_query_slot(const void* av, const void* bv)
{
  struct query_slot* a = (struct query_slot*) av;
  struct query_slot* b = (struct query_slot*) bv;

  for( int i = QUERY_LENGTH-1; i>=0; i-- ) {
    if( a->query[i] < b->query[i] ) return -1;
    if( a->query[i] > b->query[i] ) return 1;
  }

  return 0;
}

int compare_document_entry(const void* av, const void* bv)
{
  hm_entry_t* a = (hm_entry_t*) av;
  hm_entry_t* b = (hm_entry_t*) bv;
  score_t* aval = (score_t*) a->value;
  score_t* bval = (score_t*) b->value;
  double aa = 0;
  double bb = 0;

  if( aval ) aa = aval->score;
  if( bval ) bb = bval->score;


  if( aa < bb ) return 1;
  if( aa > bb ) return -1;
  return 0;
}

void print_common( const char* str, const struct common_s *common )
{
  printf("%s [%i, %i]\n", str, (int) common->first, (int) common->last);
  for( int k = 0; k < common->noffsets; k++ ) {
    my_offset_t offset = nextS[common->offsets_start_in_S+k];
    printf(" %i  ", (int) offset);
    for( int h = 0; h < common->common; h++ ) {
      int ch = getbyte_at(offset-common->common+1+h);
      printf("%02x ", ch);
    }
    for( int h = 0; h < common->common; h++ ) {
      int ch = getbyte_at(offset-common->common+1+h);
      if( !isprint(ch) ) ch = '.';
      printf("%c",  ch);
    }
    printf("\n");
  }
}

void report( const struct common_s *common, int current_index )
{
  error_t err;

  if( common->common >= min_match_length ) {
    struct group_s new_group;

    new_group.index = current_index;
    new_group.min_offset = 0; // updated below.
    new_group.len = common->common;
    new_group.n_offsets = common->noffsets;
    new_group.n_index_offsets = 1 + common->last - common->first;

    for( my_offset_t k = 0; k < common->noffsets; k++ ) {
      struct report_s new_report;
      new_report.offset = nextS[common->offsets_start_in_S + k] - common->common + 1; 
      if( k == 0 ) new_group.min_offset = new_report.offset;
      if( dbg ) printf("reporting %i %i\n", (int) new_report.offset, (int) new_group.len);
      new_report.group = n_groups;
      err = append_array(&n_reports, &reports,
                         sizeof(struct report_s), &new_report);
      die_if_err(err);
    }
    err = append_array(&n_groups, &groups,
                       sizeof(struct group_s), &new_group);
  }
}

void step( index_locator_t index_locator, int current_index )
{
  my_offset_t total_by_byte_space[257]; 
  my_offset_t by_byte_space[257]; 
  my_offset_t *total_by_byte = &total_by_byte_space[1];
  my_offset_t *by_byte = &by_byte_space[1];
  my_offset_t total_offsets_this_iter;
  my_offset_t sum;

  parallel_query_t pq;
  backward_search_query_t *queries = NULL;
  int n_queries = 0;

  struct common_s * commons = NULL;
  int n_commons = 0;

  struct common_s * next_commons = NULL;
  int next_n_commons = 0;

  int q;


  int iter = 0;

  int n_added;

  error_t err;

  // Set up S.
  // Create memory for the offsets.
  S = malloc(sizeof(my_offset_t)*mapped.len);
  nextS = malloc(sizeof(my_offset_t)*mapped.len);
  assert(S && nextS);

  for( my_offset_t i = 0; i < mapped.len; i++ ) {
    S[i] = i;
    nextS[i] = i;
  }

  // Set up rows_by_char.
  if( verbose > 1 || dbg ) printf("## computing rows by char\n");
  if( verbose > 1 ) start_clock();

  {
    parallel_query_t cpq;
    err = setup_parallel_query(&cpq, NULL, index_locator,
                                    sizeof(string_query_t),
                                    256);
    die_if_err(err);
    for( int j = 0; j < 256; j++ ) {
      string_query_t* sq = (string_query_t*) ith_query(&cpq, j);
      alpha_t alpha = CHARACTER_OFFSET + j;
      err = setup_string_query(sq, &cpq.proc, index_locator, 1, &alpha);
    }

    femto_run_query(&srv, (query_entry_t*) &cpq);


    for( int j = 0; j < 256; j++ ) {
      string_query_t* sq = (string_query_t*) ith_query(&cpq, j);
      rows_by_char[j].first = sq->first;
      rows_by_char[j].last = sq->last;
      if( dbg ) printf("%i [%i, %i]\n", j, (int) sq->first, (int) sq->last);
    }
    cleanup_generic((query_entry_t*) &cpq);
  }
  
  if( verbose > 1 ) {
    stop_clock();
    print_timings("computed rows by char", 1);
  }
 
  n_commons = 1;
  commons = malloc(sizeof(struct common_s));
  commons[0].common = 0;
  commons[0].noffsets = mapped.len;
  commons[0].offsets_start_in_S = 0;
  commons[0].first = -1;
  commons[0].last = -2;

  while(n_commons > 0) {

    if( verbose > 1 ) start_clock();

    if( verbose > 2 ) start_clock();

    total_offsets_this_iter = 0;

    for( int c = 0; c < n_commons; c++ ) {
      const struct common_s common = commons[c];

      // count the number of times each byte occurs
      for( int j = -1; j < 256; j++ ) {
        total_by_byte[j] = 0;
      }
      for( my_offset_t i = 0; i < common.noffsets; i++ ) {
        int byte = getbyte(&common, i);
        total_by_byte[byte]++;
        total_offsets_this_iter++;
      }
     
      for( int j = 0; j < 256; j++ ) {
        if( total_by_byte[j] > 0 ) {
          if( common.common > 0 ) { // 0 in common -> use rows_by_char.
            backward_search_query_t subq;
            err = setup_backward_search_query(&subq,
                                              &pq.proc,
                                              index_locator,
                                              common.first, common.last,
                                              j + CHARACTER_OFFSET);
            die_if_err(err);
            err = append_array(&n_queries, &queries,
                               sizeof(backward_search_query_t), &subq);
            die_if_err(err);
          }
        }
      }
    }

    if( verbose > 2 ) {
      stop_clock();
      print_timings("count previous", n_commons);
    }

    // Now create the parallel query and run it.
    if( n_queries > 0 ) {
      err = setup_parallel_query_take(&pq, NULL, index_locator,
                                      sizeof(backward_search_query_t),
                                      n_queries, queries);
      die_if_err(err);

      if( verbose > 1 || dbg )
        printf("## step %i running %li queries for %li common and %li offsets\n", iter, (long int) n_queries, (long int) n_commons, (long int) total_offsets_this_iter);

      if( verbose > 2 ) start_clock();

      err = femto_run_query(&srv, (query_entry_t*) &pq);
      die_if_err(err);
      
      if( verbose > 2 ) {
        stop_clock();
        print_timings("parallel queries", n_queries);
      }
    }

    if( verbose > 2 ) {
      start_clock();
    }

    // Consider stopping.
    if( iter >= stop_same_match_length &&
        iter >= min_match_length &&
        total_offsets_this_iter >= 0.9 * input_length ) {
      max_match_length = iter;
    }

    // Now go back through the results and the commons.
    q = 0;
    for( int c = 0; c < n_commons; c++ ) {
      struct common_s common = commons[c];

      // count the number of times each byte occurs
      for( int j = -1; j < 256; j++ ) {
        total_by_byte[j] = 0;
      }
      for( my_offset_t i = 0; i < common.noffsets; i++ ) {
        int byte = getbyte(&common, i);
        total_by_byte[byte]++;
      }
   
      // Now compute partial sums
      sum = 0;
      for( int j = -1; j < 256; j++ ) {
        by_byte[j] = sum;
        sum += total_by_byte[j];
      }

      // Now distribute data by previous character.
      for( my_offset_t i = 0; i < common.noffsets; i++ ) {
        int byte = getbyte(&common, i);
        nextS[common.offsets_start_in_S + by_byte[byte]++] =
          S[common.offsets_start_in_S + i];
      }

      // Now go through the queries for this one.
      sum = 0;
      n_added = 0;
      if( common.common < max_match_length ) {
        for( int j = -1; j < 256; j++ ) {
          if( total_by_byte[j] > 0 ) {
            struct common_s new_common = common;

            new_common.common++;
            new_common.noffsets = total_by_byte[j];
            new_common.offsets_start_in_S += sum;

            if( new_common.common == 1 ) {
              new_common.first = rows_by_char[j].first;
              new_common.last = rows_by_char[j].last;
            } else if( j == -1 ) {
              new_common.first = -1;
              new_common.last = -2;
            } else {
              assert( common.first == queries[q].first_in &&
                      common.last == queries[q].last_in );
            
              new_common.first = queries[q].first;
              new_common.last = queries[q].last;
            
              // Move on to the next query.
              q++;
            }

            if( dbg ) print_common( "got ", &new_common);

            if( new_common.first <= new_common.last ) {
              err = append_array(&next_n_commons, &next_commons,
                                 sizeof(struct common_s), &new_common);
              n_added++;
  
              /*{
                my_offset_t offset = S[new_common.offsets_start_in_S];
                printf("intermediate match\n");
                for( my_offset_t j = 0; j < new_common.common; j++ ) {
                  printf("%02x ", mapped.data[offset-new_common.common+1+j]);
                }
                printf("\n");
              }*/
   

            }
            sum += total_by_byte[j];
          }
        }
      }

      if( n_added == 0 ) {
        if( dbg ) print_common( "report ", &common);
        // we did not extend that pattern
        // We reached the end of the match. Report.
        report(&common, current_index);
      }
      // Move on to the next 'commons'
    }


    if( verbose > 2 ) {
      stop_clock();
      print_timings("extend queries", n_queries);
    }

    if( verbose > 1 ) {
      stop_clock();
      print_timings("step", n_queries);
    }


    // Free queries...
    if( n_queries > 0 ) cleanup_generic((query_entry_t*) &pq);
    queries = NULL;
    n_queries = 0;

    // Switch next commons to commons, next S to S.
    free(commons);
    n_commons = 0;
    commons = next_commons;
    n_commons = next_n_commons;
    next_commons = NULL;
    next_n_commons = 0;

    {
      my_offset_t* tmp = S;
      S = nextS;
      nextS = tmp;
    }

    iter++;
  }
}

void usage(char* pname)
{
  printf("Usage: %s [options] <index> [<more indexes>..] file-to-query\n", 
         pname);
  printf("where [options] can be placed anywhere and include:\n"
         "   --not <not_index>\n"
         "       omit matches in not_index (can be specified more than once)\n"
         "   --min-match-length <num>\n"
         "       set the minimum match length in bytes (default is %i)\n",
        min_match_length);
  printf("   --max-match-length <num>\n"
         "       set the maximum match length in bytes (default is unbounded)\n"
         "   --too-similar-stop <num>\n"
         "       stop if 90%% of document matches after exploring this length\n"
         "   --matches\n"
         "       show the common byte sequences found\n"
         "   --no-align-ascii\n"
         "       don't add spaces to align ascii with hexadecimal\n"
         "   --offsets\n"
         "       show offsets of matches found (implies --matches)\n"
         "   --document-score\n"
         "       show similarity to documents\n"
         "   --index-score\n"
         "       show similarity to index\n"
         "   --no-total-score\n"
         "       do not show total similarity to all indexes\n"
         "   --no-remove-redundant\n"
         "       do not remove overlapping matches\n"
         "       (removing them prevents symmetry in # sequences found)\n"
         "\n"
         "Returns information like:\n"
         "  total score SCORE : MIN <= len <= MAX : NSEQ sequences : NBYTES bytes\n"
         "where:\n"
         "  SCORE is a similarity score = NBYTES/len\n"
         "        (where len is input length for document-index scores or\n"
         "         min(input length, related document length) for document scores\n"
         "  MIN is the minimum common sequence length found\n"
         "  MAX is the maximum common sequence length found\n"
         "  NSEQ is the number of unique sequences found in common\n"
         " NBYTES is the number of bytes represented by the found common sequences.\n"
         "        It can be > input length with overlaps and is\n"
         "        sum(len[i]*min(noccs-input[i],noocs-index[i])) where i ranges over NSEQ\n"
         "\n"
      );
  exit(-1);
}

int main(int argc, char** argv)
{
  char* filename = NULL;
  FILE* f;
  error_t err;
  int matches = 0;
  int offsets = 0;
  int document_score = 0;
  int index_score = 0;
  int total_score = 1;
  int remove_redundant = 1;
  int align_ascii = 1;

  char** index_paths = NULL;
  int num_indexes = 0;
  index_paths = (char**) malloc(argc*sizeof(char*));
  not_index_paths = (char**) malloc(argc*sizeof(char*));

  {
    int i = 1;
    while( i < argc ) {
      // process options
      if( 0 == strcmp(argv[i], "-v") ||
          0 == strcmp(argv[i], "--verbose") ) {
        verbose++;
      } else if( 0 == strcmp(argv[i], "--not") ) {
        i++;
        not_index_paths[num_not_indexes++] = argv[i];
      } else if( 0 == strcmp(argv[i], "--min-match-length") ) {
        i++;
        min_match_length = atoi(argv[i]);
      } else if( 0 == strcmp(argv[i], "--max-match-length") ) {
        i++;
        max_match_length = atoi(argv[i]);
      } else if( 0 == strcmp(argv[i], "--too-similar-stop") ) {
        i++;
        stop_same_match_length = atoi(argv[i]);
      } else if( 0 == strcmp(argv[i], "--matches") ) {
        matches = 1;
      } else if( 0 == strcmp(argv[i], "--no-align-ascii") ) {
        align_ascii = 0;
      } else if( 0 == strcmp(argv[i], "--offsets") ) {
        matches = 1;
        offsets = 1;
      } else if( 0 == strcmp(argv[i], "--document-score") ) {
        document_score = 1;
      } else if( 0 == strcmp(argv[i], "--index-score") ) {
        index_score = 1;
      } else if( 0 == strcmp(argv[i], "--no-total-score") ) {
        total_score = 0;
      } else if( 0 == strcmp(argv[i], "--no-remove-redundant") ) {
        remove_redundant = 0;
      } else if( 0 == strcmp(argv[i], "--help") ||
                 0 == strcmp(argv[i], "-h") ) {
        usage(argv[0]);
      } else if( argv[i][0] == '-' ) {
        fprintf(stderr, "Unknown option %s\n", argv[i]);
        usage(argv[0]);
      } else {
        index_paths[num_indexes++] = argv[i];
      }
      i++;
    }
  }

  num_indexes--;
  filename = index_paths[num_indexes];
  
  if( num_indexes <= 0 ) {
    fprintf(stderr, "No indexes provided\n");
    usage(argv[0]);
  }

  f = fopen(filename, "r");
  assert(f);

  // Now... memory map the file.
  err = mmap_buffer(&mapped, f, 0, 0);
  die_if_err(err);

  input_length = mapped.len;

  if( input_length != mapped.len ) {
    fprintf(stderr, "Input file too large (change my_offset_t to work with larger input, but you might run out of memory!");
    exit(-1);
  }

  err = femto_start_server_err(&srv, 0);
  die_if_err( err );

  // Total time...
  if( verbose ) start_clock();

  if( verbose > 1 ) start_clock();

  for( int idx = 0; idx < num_indexes; idx++ ) {
    index_locator_t loc;

    // Get the locator for the index we're using.
    err = femto_loc_for_path_err(&srv, index_paths[idx], &loc);
    die_if_err( err );

    step(loc, idx);
  }

  if( verbose > 1 ) {
    stop_clock();
    print_timings("form matches", mapped.len);
  }


  if( verbose ) {
    printf("Found %i results before removing overlaps\n", n_reports);
  }

  if( verbose > 1 ) start_clock();

  // Now, we no longer need S or nextS
  free(S);
  free(nextS);
  S = NULL;
  nextS = NULL;


  // Remove the results that overlap.
  if( remove_redundant ) {
    my_offset_t min_start = mapped.len + 1;
    my_offset_t start, end;
    my_offset_t offset = 0;
    int drop = 0;
    int* last_group_id = NULL;
    struct group_s* lastgroup = NULL;
    unsigned char* lastptr = NULL;
    my_offset_t lastlen = 0;
    struct group_map_s* group_map = NULL;

    group_map = malloc(n_groups*sizeof(struct group_map_s));
    assert( group_map );

    for( int j = 0; j < n_groups; j++ ) {
      group_map[j].id = 0;
      group_map[j].new_id = 0;
    }
    // Sort the results by offset.
    qsort(reports, n_reports, sizeof(struct report_s), rev_compare_report);

    for( int i = 0; i < n_reports; i++ ) {
      struct report_s* currep = &reports[i];
      struct group_s* curgroup = &groups[currep->group];
      
      start = currep->offset;
      end = start + curgroup->len;
      offset = start;

      // Don't report overlapping matches.
      if( min_start <= start ) {
        if( dbg ) printf("skipping ");
        drop = 1;
      } else {
        min_start = start;
        drop = 0;
      }

      if( drop ) {
        // Mark this one as overlap
        currep->offset = mapped.len + 1;
        currep->group = n_groups + 1;
      } else {
        // Add to group so that we keep these groups
        group_map[currep->group].new_id++;
      }

      if( dbg ) {
        printf("match %i-%i\n", (int) start, (int) end);
        printf("intermediate result %li drop %i\n", (long) offset, drop);
        for( my_offset_t j = 0; j < curgroup->len; j++ ) {
          printf("%02x ", mapped.data[offset+j]);
        }
        printf("\n");
      }
    }

    // Now group_map[j] has a count in it. Change it to
    // preserve the group if it exists or -1 if we can
    // remove that group.
    for( int j = 0; j < n_groups; j++ ) {
      group_map[j].id = j;
      if( group_map[j].new_id > 0 ) {
        group_map[j].new_id = j;
      } else {
        // mark it as removed
        groups[j].n_offsets = 0;
        group_map[j].new_id = n_groups + 1;
      }
    }

    /*
    for( int j = 0; j < n_groups; j++ ) {
      printf("map %i to %i\n", group_map[j].id, group_map[j].new_id);
    }*/
 
    // Now sort the results by pattern.
    qsort(group_map, n_groups, sizeof(struct group_map_s), compare_group_id_text);
    for( int j = 0; j < n_groups; j++ ) {
      int* cur_group_id = NULL;
      struct group_s* curgroup;
      unsigned char* curptr;
      my_offset_t curlen;

      // Skip the ones we already removed.
      if( group_map[j].new_id >= n_groups ) break;

      cur_group_id = &group_map[j].new_id;
      curgroup = &groups[*cur_group_id];
      curptr = &mapped.data[curgroup->min_offset];
      curlen = curgroup->len;

      if( dbg ) {
        printf("considering group %i of len %i\n", (int) *cur_group_id, (int) curlen);
        for( my_offset_t k = 0; k < curlen; k++ ) {
          printf("%02x ", curptr[k]);
        }
        printf("\n");
      }

      if( lastlen > 0 && lastlen <= curlen &&
          0 == memcmp(lastptr, curptr, lastlen) ) {
        // lastgroup is a prefix of curgroup
        if( dbg ) {
          printf("dropping overlapping result (%i bytes prefix of %i bytes)\n", (int) lastlen, curlen);
          for( my_offset_t k = 0; k < lastlen; k++ ) {
            printf("%02x ", lastptr[k]);
          }
          printf("\n");
        }
        if( lastlen == curlen ) {
          // redirect the last group to this group.
          //printf("Moving %i to %i\n", *last_group_id, *cur_group_id);
          *last_group_id = *cur_group_id;
          curgroup->n_offsets += lastgroup->n_offsets;
          lastgroup->n_offsets = 0;
        } else if( lastgroup->index == curgroup->index) {
          // remove last group since it is overlapping
          // and therefore a duplicate report.
          // mark it as removed.
          //printf("Clearing %i\n", *last_group_id);
          lastgroup->n_offsets = 0;
          *last_group_id = n_groups + 1;
        }
      }

      //printf("after: %i maps to %i\n", group_map[j].id, group_map[j].new_id);

      last_group_id = cur_group_id;
      lastgroup = curgroup;
      lastptr = curptr;
      lastlen = curlen;
    }

    qsort(group_map, n_groups, sizeof(struct group_map_s), compare_group_id);

    /*
    for( int j = 0; j < n_groups; j++ ) {
      printf("map %i to %i\n", group_map[j].id, group_map[j].new_id);
    }

    for( int i = 0; i < n_reports; i++ ) {
      struct report_s* currep = &reports[i];
      printf("report %i of group %i offset %i\n",
             i, currep->group, currep->offset);
    }*/

    // Now redirect reports to the correct group.
    for( int i = 0; i < n_reports; i++ ) {
      struct report_s* currep = &reports[i];
      if( currep->group < n_groups ) {
        currep->group = group_map[currep->group].new_id;
      }
      if( currep->group >= n_groups ) {
        currep->offset = mapped.len + 1;
      }
    }

    free(group_map);

    /*
    for( int j = 0; j < n_groups; j++ ) {
      struct group_s* curgroup = &groups[j];
      unsigned char* curptr = &mapped.data[curgroup->min_offset];
      my_offset_t curlen = curgroup->len;
      printf("group %i of len %i n_offsets %i\n", (int) j, (int) curlen,
           curgroup->n_offsets);
      for( my_offset_t k = 0; k < curlen; k++ ) {
        printf("%02x ", curptr[k]);
      }
      printf("\n");
    }

    for( int i = 0; i < n_reports; i++ ) {
      struct report_s* currep = &reports[i];
      printf("report %i of group %i offset %i\n",
             i, currep->group, currep->offset);
    }*/


  }

  // Sort the results by group and offset.
  qsort(reports, n_reports, sizeof(struct report_s), compare_report);

  // get ready to run two sets of queries:
  // - filter out using --not
  // - find offsets in index

  if( verbose > 1 || dbg ) printf("## Creating T\n"); 
  // To prepare to do that, we allocate an alpha_t* T
  // and set it to the input.
  alpha_t *T = malloc(sizeof(alpha_t)*mapped.len);
  assert(T);
  for( my_offset_t i = 0; i < mapped.len; i++ ) {
    T[i] = CHARACTER_OFFSET + mapped.data[i];
  }

  if( verbose > 1 ) {
    stop_clock();
    print_timings("remove overlaps and create T", mapped.len);
  }



  // Filter results with --not indexes.
  if( num_not_indexes > 0 ) {
    my_offset_t offset, len;
    int n_patterns = 0;
    int patterns_i = 0;

    if( verbose > 1 ) start_clock();

    for( int j = 0; j < n_groups; j++ ) {
      // Skip the overlaps...
      if( groups[j].n_offsets == 0 ) continue;

      n_patterns++;
    }

    // Now filter them by --not indices
    for( int idx = 0; idx < num_not_indexes; idx++ ) {
      index_locator_t loc;
      parallel_query_t pcount;
      int ok;

      // don't bother if there are no patterns
      if( n_patterns == 0 ) break;

      err = femto_loc_for_path_err(&srv, not_index_paths[idx], &loc);
      die_if_err( err );

      err = setup_parallel_query(&pcount, NULL, loc,
                                 sizeof(string_query_t),
                                 n_patterns);
      patterns_i = 0;
      for( int j = 0; j < n_groups; j++ ) {
        // Skip the overlaps...
        if( groups[j].n_offsets == 0 ) continue;

        offset = groups[j].min_offset;
        len = groups[j].len;

        {
          alpha_t* pat = &T[offset];

          string_query_t* sq = (string_query_t*) ith_query(&pcount, patterns_i);
          setup_string_query_take(sq, &pcount.proc, loc, len, pat);

          patterns_i++;
        }
      }

      if( verbose > 1 || dbg ) printf("## checking --not against %i patterns\n", n_patterns);

      femto_run_query(&srv, (query_entry_t*) &pcount);

      patterns_i = 0;
      ok = 0;
      for( int j = 0; j < n_groups; j++ ) {
        // Skip the overlaps...
        if( groups[j].n_offsets == 0 ) continue;

        offset = groups[j].min_offset;

        {
          string_query_t* sq = (string_query_t*) ith_query(&pcount, patterns_i);
          alpha_t* pat = &T[offset];

          assert( pat == sq->pat );

          ok = 1;
          if( sq->first <= sq->last ) {
            // It occurs in the not index! Filter it out.
            ok = 0;
          }
          patterns_i++;
        }
        if( ! ok ) {
          // Mark the group as empty
          groups[j].n_offsets = 0;
        }
      }
      for( int i = 0; i < n_patterns; i++ ) {
        string_query_t* sq = (string_query_t*) ith_query(&pcount, i);
        sq->pat = NULL; // don't free it!
      }
      cleanup_parallel_query(&pcount);
    }
  
    if( verbose > 1 ) {
      stop_clock();
      print_timings("filter with not indexes", n_patterns);
    }
  }

  // Now show the results.
  {
    my_offset_t last_group = n_groups;
    my_offset_t offset, len;
    hashmap_t score_by_document;
    hash_t score_by_document_size;
    hm_entry_t entry;
    score_t *by_index = NULL;
    int report_index;
    my_offset_t report_noffsets;
    my_offset_t report_n_index_offsets;
    my_offset_t total_offsets_reported = 0;
    my_offset_t total_length_reported = 0;
    score_t total_common;
    int n_matches = 0;
   
    if( verbose > 1 || dbg ) printf("## gathering results\n");

    init_score(&total_common);

    if( index_score ) {
      by_index = malloc(sizeof(score_t)*num_indexes);
      for( int idx = 0; idx < num_indexes; idx++ ) {
        init_score(&by_index[idx]);
      }
    }

    if( document_score ) {
      score_by_document_size = hashmap_size_for_elements(10*n_reports);
      err = hashmap_create(&score_by_document, score_by_document_size,
                           hash_string_fn, hash_string_cmp);
      die_if_err(err);
    }
    
    if( verbose > 1 ) start_clock();

    offset = 0;
    for( int i = 0; i < n_reports; i++ ) {
      struct report_s* currep = &reports[i];
      struct group_s* curgroup = &groups[currep->group];

      // Skip the overlaps and the filtered-out...
      if( currep->offset >= mapped.len ) continue;
      if( currep->group >= n_groups ) continue;
      if( curgroup->n_offsets == 0 ) continue;

      offset = currep->offset;
      len = curgroup->len;
      report_index = curgroup->index;
      report_noffsets = curgroup->n_offsets;
      report_n_index_offsets = curgroup->n_index_offsets;

      // We should not have any matches too short...
      assert( len >= min_match_length );

      if( reports[i].group != last_group ) {
        //printf("group %i len %i noffsets %i\n",
        //       (int) reports[i].group,
        //       (int) len,
        //       (int) report_noffsets);

        // Gather information on amount matched...
        if( len > max_got_match_length ) max_got_match_length = len;
        if( by_index ) {
          accumulate_score(&by_index[report_index], len,
                           report_noffsets, report_n_index_offsets, 0);
        }
        accumulate_score(&total_common, len,
                         report_noffsets, report_n_index_offsets, 0);
        total_offsets_reported += report_noffsets;
        total_length_reported += len*report_noffsets;

        if( offsets && i != 0 ) printf("\n");

        // Print out the group header.
        if( matches ) {
          printf("# (%li bytes)\n", (long int) len);
          //printf("# group %i\n", (int) reports[i].group);
          for( my_offset_t j = 0; j < len; j++ ) {
            printf("%02x ", mapped.data[offset+j]);
          }
          printf("\n");
          for( my_offset_t j = 0; j < len; j++ ) {
            char ch = mapped.data[offset+j];
            if( !isprint(ch) ) ch = '.';
            if( ch == '\n' ) ch = ' ';
            if( ch == '\t' ) ch = ' ';
            printf(align_ascii?" %c ":"%c", ch);
          }
          printf("\n");
        }
        n_matches++;


        // Move on if we're not reporting offsets.
        if( (offsets||document_score) ) {

          if( offsets ) printf(" index offsets");
          for( int idx = 0; idx < num_indexes; idx++ ) {
            index_locator_t loc;
            string_results_query_t rq;
            result_type_t rtype;

            if( offsets ) rtype = RESULT_TYPE_DOC_OFFSETS;
            else rtype = RESULT_TYPE_DOCUMENTS;

            // Get the locator for the index we're using.
            err = femto_loc_for_path_err(&srv, index_paths[idx], &loc);
            die_if_err( err );

            err = setup_string_results_query(&rq, NULL, loc, max_offsets, rtype, len, &T[offset]);
            die_if_err( err );

            femto_run_query(&srv, (query_entry_t*) &rq);

            {
              results_t *results = &rq.results.results;
              parallel_query_t ctx;
              results_reader_t reader;
              int i, j;
              int64_t document, last_document, offset;
              unsigned char* info = NULL;
              long info_len = 0;
              int num = 0;

              long* lens = NULL;
              unsigned char **infos = NULL;
              int64_t* doc_lens = NULL;
              int64_t report_n_doc_offsets = 0;
              int64_t doc_len = 0;


              if( results_num_results(results) > 0 ) {
                err = setup_get_doc_info_doc_len_results(&ctx, NULL,
                                                         loc, results);
                die_if_err(err);

                err = femto_run_query(&srv, (query_entry_t*) &ctx);
                die_if_err(err);

                get_doc_info_doc_len_results(&ctx, &num, &lens, &infos,
                                             &doc_lens);
                cleanup_get_doc_info_results(&ctx);
              }

              //has_offsets = is_doc_offsets(results_type(results));

              last_document = 0;
              err = results_reader_create(&reader, results);
              if( err ) return err;
              i = 0;
              j = 0;
              while( results_reader_next(&reader, &document, &offset) ) {
                // is it a new document?
                if( i == 0 || last_document != document ) {
                  if( i != 0 ) {
                    // accumulate the score from the last document
                    // since we now know the number of matches.
                    assert(entry.value);
                    // Now update the score in entry.value.
                    accumulate_score((score_t*)entry.value, len,
                                     report_noffsets, report_n_doc_offsets,
                                     doc_len);
                  }

                  last_document = document;
                  info = & (infos[j][0]);
                  info_len = lens[j];
                  doc_len = doc_lens[j];
                  report_n_doc_offsets = 0;

                  if( document_score ) {
                    unsigned char *buf = malloc(info_len+1);
                    memcpy(buf, info, info_len);
                    buf[info_len] = 0;

                    // add match length to score for info into hashtable.
                    entry.key = buf;
                    entry.value = NULL;
                    if( ! hashmap_retrieve(&score_by_document,&entry) ){
                      score_t *sc = malloc(sizeof(score_t));
                      init_score(sc);
                      entry.key = buf;
                      entry.value = sc;
                      err = hashmap_resize(&score_by_document);
                      die_if_err(err);
                      err = hashmap_insert(&score_by_document, &entry);
                      die_if_err(err);
                    } else {
                      free(buf); // not a new entry, don't need new info
                    }
                  }

                  j++;
  
                  if( offsets ) {
                    printf("\n");
                    printf("  %.*s\n\t", (int) info_len, info);
                  }

                }
                if( offsets ) {
                  printf(" %" PRIi64, offset);
                }
                report_n_doc_offsets++;
                i++;
              }
              if( i != 0 ) {
                // accumulate the score from the last document
                // since we now know the number of matches.
                assert(entry.value);
                // Now update the score in entry.value.
                accumulate_score((score_t*)entry.value, len,
                                 report_noffsets, report_n_doc_offsets,
                                 doc_len);
              }


              for( i = 0; i < num; i++ ) {
                free(infos[i]);
              }
              free(infos);
              free(lens);
              free(doc_lens);
            }

            cleanup_string_results_query(&rq);
          }
        }

        if( offsets ) printf("\n file offsets\n  %s\n\t", filename);
        last_group = currep->group;
      }
      if( offsets ) printf(" %li", (long int) offset);
    }
    if( offsets ) printf("\n");

    if( document_score ) {
      // Sort the documents by accumulated score.

      qsort(score_by_document.map, score_by_document.size, sizeof(hm_entry_t),
            compare_document_entry);
      printf("# document scores\n");
      for( hash_t slot = 0; slot < score_by_document.size; slot++ ) {
        if( score_by_document.map[slot].key ) {
          void* key = score_by_document.map[slot].key;
          void* value = score_by_document.map[slot].value;
          char* name = (char*) key;
          score_t* scorep = (score_t*) value;
         
          print_score(name, scorep);
        
          free(key);
          free(value);
        }
      }

      hashmap_destroy(&score_by_document);
    }

    if( by_index ) {
      printf("# index scores\n");
      for( int idx = 0; idx < num_indexes; idx++ ) {
        print_score(index_paths[idx], &by_index[idx]);
      }
      free(by_index);
    }

    if( total_score ) {
      print_score("total score", &total_common);
    }

    if( verbose > 1 ) {
      stop_clock();
      print_timings("report results", 1);
    }

  }

  if( verbose ) {
    stop_clock();
    print_timings("total time", 1);
  }

  err = munmap_buffer(&mapped);

  fclose(f);

  free(index_paths);
  free(not_index_paths);
  free(T);
}

