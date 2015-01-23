/*
  (*) 2006-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/wtree.h
*/
#ifndef _WTREE_H_
#define _WTREE_H_ 1

#include "error.h"
#include "buffer.h"
#include "bswap.h"

// Track runs up to this size for statistics..
#define TRACK_RUNS_DIST 15
//#define TRACK_DENSITY_BITS 63
//#define TRACK_DENSITY_DIST (TRACK_DENSITY_BITS+1)

typedef struct {
  long segments;
  long num_rle_segments;
  long num_unc_segments;
  long segment_sums;
  long group_zeros;
  long group_ones;
  long group_ptrs;
  long num_runs;
  long run_count[TRACK_RUNS_DIST];
  int max_run;
  long segment_bits;
  long segment_bits_used;
  long rle_segment_bits_used;
  long unc_segment_bits_used;
#ifdef TRACK_DENSITY_DIST
  long density_count64[TRACK_DENSITY_DIST];
  long density_count64_zero_run[TRACK_RUNS_DIST];
  long density_count64_one_run[TRACK_RUNS_DIST];
#endif
} bseq_stats_t;

typedef struct {
  long offs;
  long bseqs;
  bseq_stats_t bseq_stats;
} wtree_stats_t;

error_t wtree_construct(int* zlen, unsigned char** zdata,
                        int nInUse, int* leaf_map,
                        int len, int* data, wtree_stats_t* stats);


typedef struct {
  int index; // the index of the character -
             // used by wtree_rank and wtree_occs, set by wtree_select,
             // counted from 1.
  int leaf; // the leaf node number -
            // set by wtree_rank
            // used by wtree_occs and wtree_select
            // counted from 1+internal nodes.
  int count; // the number of occurences of the leaf node character
             // at and before the index;
             // set by wtree_rank and wtree_occs, used by wtree_select
             // counted from 1.
} wtree_query_t;

error_t wtree_rank(unsigned char* zdata, wtree_query_t* q);
error_t wtree_occs(unsigned char* zdata, wtree_query_t* q);
error_t wtree_select(unsigned char* zdata, wtree_query_t* q);

int wtree_settings_number(void);

#endif
