/*
  (*) 2007-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/two_stage.cc
*/

#include <cassert>

extern "C" {
  #include "suffix_sort.h"
}

#define DEBUG 0

/* First of all, let's get a basic implementation of two-stage going. */
static inline
error_t two_stage_single_impl(const suffix_sorting_problem_t* p,
                              suffix_context_t* context,
                              size_t str_len,
                              compare_fun_t compare,
                              sptr_t max_char, // the maximum character value..
                              // These args are here just for optimizing.
                              int bytes_per_character,
                              int bytes_per_pointer)
{
  unsigned char** L_buckets; // these are pointers into S.
  unsigned char** S_buckets;
  int n_buckets;
  error_t err;
  sptr_t tlast;
  char sorting_S;

  assert(p->S);

  if( max_char <= 0 ) {
    n_buckets = 1 << (8*bytes_per_character);
  } else {
    n_buckets = max_char;
  }

  L_buckets = (unsigned char**) calloc(n_buckets, sizeof(unsigned char*));
  if( ! L_buckets ) return ERR_MEM;
  S_buckets = (unsigned char**) calloc(n_buckets, sizeof(unsigned char*));
  if( ! S_buckets ) return ERR_MEM;
  // now all buckets are initialized to 0.

  // Partition into buckets.
  // First, count the number bytes in each
#define CLASSIFY_SL(S_todo,L_todo) \
{ \
  sptr_t ti, tend; \
  /* tend starts out as the first character. */ \
  tend = get_T(p->T, bytes_per_character, 0); \
  for( sptr_t end, to = 0; \
       to < p->t_size; ) { \
    ti = tend; \
    /* find the smallest end>to such that ti_end!=ti_to. */ \
    for( end = to+bytes_per_character; \
         end < p->t_size; \
         end += bytes_per_character ) { \
      tend = get_T(p->T, bytes_per_character, end); \
      if( ti != tend ) break; \
    } \
    if( end == p->t_size ) { \
      /* The end of the string is smaller than any other. */ \
      tend = -1; /* a character that won't actually occur..*/ \
    } \
    if( ti < tend ) { \
      /* suffixes to...end are type S */ \
      for( ; to < end; to += bytes_per_character ) { \
        if( DEBUG > 10 ) { \
          ti = get_T(p->T, bytes_per_character, to); \
          assert(0 <= ti && ti < n_buckets); \
        } \
        S_todo; \
      } \
    } else { /* ti > tend */ \
      /* suffixes to...end are type L */ \
      for( ; to < end; to += bytes_per_character ) { \
        if( DEBUG > 10 ) { \
          ti = get_T(p->T, bytes_per_character, to); \
          assert(0 <= ti && ti < n_buckets); \
        } \
        L_todo; \
      } \
    } \
  } \
}


  {
    sptr_t total_S, total_L;
    total_S = 0;
    CLASSIFY_SL({
                  S_buckets[ti] += bytes_per_pointer;
                  total_S++;
                },
                {
                  L_buckets[ti] += bytes_per_pointer;
                });
    total_L = p->n - total_S;
    if( total_L < total_S ) {
      sorting_S = 0;
    } else {
      sorting_S = 1;
    }
  }

  // Next, turn the buckets into offsets in the input.
  // Make L_bucket and S_bucket point to the *end* of each bucket
  // and the L buckets appear before the S buckets.
  {
    unsigned char* next = p->S;
    sptr_t count;
    for( int j = 0; j < n_buckets; j++ ) {
      count = (sptr_t) L_buckets[j];
      next += count;
      L_buckets[j] = next;
      count = (sptr_t) S_buckets[j];
      next += count;
      S_buckets[j] = next;
    }
  }

  // Now sort the original text into the L-and S-buckets.
  // note that the buckets are now pointing at the ends of the buckets..
  {
    char mark_next = 0;
    // store the bit-complement on the next bucket if this 
    // bucket is of the sorting type.
    CLASSIFY_SL({
                  sptr_t store;
                  store = (mark_next)?(~to):(to);
                  mark_next = !sorting_S;
                  S_buckets[ti] -= bytes_per_pointer;
                  if( DEBUG > 10 ) {
                    if(p->S <= S_buckets[ti] &&
                               S_buckets[ti] <= p->S + p->s_size ) {
                    } else {
                      assert(p->S <= S_buckets[ti] &&
                                     S_buckets[ti] <= p->S + p->s_size );
                    }
                  }
                  set_S_with_ptr(bytes_per_pointer, S_buckets[ti], store);
                },
                {
                  sptr_t store;
                  store = (mark_next)?(~to):(to);
                  mark_next = sorting_S;
                  L_buckets[ti] -= bytes_per_pointer;
                  if( DEBUG > 10 ) {
                    if(p->S <= L_buckets[ti] &&
                               L_buckets[ti] <= p->S + p->s_size ) {
                    } else {
                      assert(p->S <= L_buckets[ti] &&
                                     L_buckets[ti] <= p->S + p->s_size );
                    }
                  }
                  set_S_with_ptr(bytes_per_pointer, L_buckets[ti], store);
                });

  }

  // just save the last character - we'll need it in a minute.
  // Note that since CLASSIFY_SL goes forward, the last suffix
  // should be at the top of its L bucket, which is the correct position.
  tlast = get_T(p->T, bytes_per_character, p->t_size - bytes_per_character);

  // Now sort all of the b-buckets.
  // L_buckets are currently pointing to the start of the L_bucket.
  // S_buckets are currently pointing to the start of the S_bucket.
  {
    string_sort_params_t local;

    local.context = context;
    local.base = NULL;
    local.n_memb = 0;
    local.memb_size = bytes_per_pointer;
    local.str_len = str_len;
    local.same_depth = bytes_per_character; // we've already sorted one char.
    local.get_string = get_string_getter_comp(context);
    local.compare = compare;

    if( sorting_S ) {
      // TODO OMP PARALLEL FOR
      for( int j = 0; j < n_buckets; j++ ) {
        unsigned char* end;
        string_sort_params_t mylocal = local;
        if( j < n_buckets - 1 ) {
          end = L_buckets[j+1];
        } else {
          // the last bucket ends at the end of p->S
          end = p->S+p->s_size;
        }

        // sort from S_buckets[j] to end
        mylocal.base = S_buckets[j];
        mylocal.n_memb = (end - mylocal.base)/bytes_per_pointer;
        if( mylocal.n_memb > 1 ) {
          // don't bother calling sort for zero-sized or 1-sized buckets.
          err = favorite_string_sort(&mylocal);
          if( err ) return err;
        }
      }
    } else { // sorting L
      // TODO OMP PARALLEL FOR
      for( int j = 0; j < n_buckets; j++ ) {
        unsigned char* end;
        string_sort_params_t mylocal = local;
        end = S_buckets[j];
        // sort from L_buckets[j] to end
        mylocal.base = L_buckets[j];
        // if we're working with the bucket containing the last
        // suffix, don't sort the first entry.
        if( j == tlast ) mylocal.base += bytes_per_pointer;
        mylocal.n_memb = (end - mylocal.base)/bytes_per_pointer;
        if( mylocal.n_memb > 1 ) {
          // don't bother calling sort for zero-sized or 1-sized buckets.
          err = favorite_string_sort(&mylocal);
          if( err ) return err;
        }
      }

      // when sorting type L suffixes, we really want the S-bucket
      // pointers to point to the end of the S-bucket.
      for( int j = 0; j < n_buckets; j++ ) {
        unsigned char* end;
        if( j < n_buckets - 1 ) {
          end = L_buckets[j+1];
        } else {
          // the last bucket ends at the end of p->S
          end = p->S+p->s_size;
        }
        S_buckets[j] = end;
      }
    }
  }

  // Now compute the final ordering of the suffix array
  // We've marked suffixes (with bit complement) if:
  // for suffix S[i], S[i]-1 is a type-that-we-sorted (e.g. L)
  // Note that this could go by bucket instead of with the marking
  if( sorting_S ) {
    // Sorting S-type - scan the suffixes from left to right and
    // append the data to the starts of the L-buckets.
    // We need to keep the last suffix at the top of it's L bucket.
    L_buckets[tlast] += bytes_per_pointer;

    for( sptr_t so = 0; so < p->s_size; so += bytes_per_pointer ) {
      // is S[i]-1 an L-type suffix?
      sptr_t to, tprev, ti;
      to = get_S(bytes_per_pointer, p->S, so);
      if( to >= 0 ) continue;
      // then it's a marked suffix. Note that suffix 0 is never marked.
      // marked means that S[i]-1 is an L-type suffix.
      to = ~ to;
      //tnext = get_T(p->T, bytes_per_character, to);
      to -= bytes_per_character;
      ti = get_T(p->T, bytes_per_character, to);
      // mark it if it's preceeded by a type L suffix.
      if( to > 0 ) {
        tprev = get_T(p->T, bytes_per_character, to - bytes_per_character);
        if( tprev >= ti ) to = ~ to;
      }
      // copy to the start of the appropriate L-bucket,
      set_S_with_ptr(bytes_per_pointer, L_buckets[ti], to);
      L_buckets[ti] += bytes_per_pointer;
    }
  } else {
    // sorting L
    // we must scan the suffixes from right to left and append to the ends
    // of the S-buckets.
    for( sptr_t so = p->s_size - bytes_per_pointer;
         so >= 0;
         so -= bytes_per_pointer) {
      // is S[i]-1 an S-type suffix?
      sptr_t to, tprev, ti;
      to = get_S(bytes_per_pointer, p->S, so);
      if( to >= 0 ) continue;
      // then it's a marked suffix. Note that suffix 0 is never marked.
      // marked means that S[i]-1 is an S-type suffix.
      to = ~ to;
      //tnext = get_T(p->T, bytes_per_character, to);
      to -= bytes_per_character;
      ti = get_T(p->T, bytes_per_character, to);
      // mark it if it's preceeded by a type S suffix.
      if( to > 0 ) {
        tprev = get_T(p->T, bytes_per_character, to - bytes_per_character);
        if( tprev <= ti ) to = ~ to;
      }
      // copy to the end of the appropriate S-bucket.
      S_buckets[ti] -= bytes_per_pointer;
      set_S_with_ptr(bytes_per_pointer, S_buckets[ti], to);
    }
  }

  // unmark any marked suffixes.
  for( sptr_t so = 0; so < p->s_size; so += bytes_per_pointer ) {
    sptr_t to;
    to = get_S(bytes_per_pointer, p->S, so);
    if( to >= 0 ) continue;
    to = ~to;
    set_S(bytes_per_pointer, p->S, so, to);
  }

  free(S_buckets);
  free(L_buckets);

  return ERR_NOERR;
#undef CLASSIFY_SL
}

// There's really only a handful of bytes-per-pointer that could 
// reasonably be used. This is just is an attempt to get optimized code for each.
error_t two_stage_single(suffix_sorting_problem_t* p,
                         suffix_context_t* context, size_t str_len, compare_fun_t compare, sptr_t max_char)
{
#define TSS(b_per_c, b_per_p) if( p->bytes_per_character == b_per_c && p->bytes_per_pointer == b_per_p ) return two_stage_single_impl(p, context, str_len, compare, max_char, b_per_c, b_per_p);

  TSS(1,1);
  TSS(1,2);
  TSS(1,3);
  TSS(1,4);
  TSS(1,5);
  TSS(2,1);
  TSS(2,2);
  TSS(2,3);
  TSS(2,4);
  TSS(2,5);
  TSS(3,1);
  TSS(3,2);
  TSS(3,3);
  TSS(3,4);
  TSS(3,5);
  TSS(4,1);
  TSS(4,2);
  TSS(4,3);
  TSS(4,4);
  TSS(4,5);
  TSS(5,1);
  TSS(5,2);
  TSS(5,3);
  TSS(5,4);
  TSS(5,5);
  TSS(p->bytes_per_character, p->bytes_per_pointer);
#undef TSS
}

/* two-stage double-implementation... ala Yuta Mori */
static inline
error_t two_stage_double_impl(suffix_sorting_problem_t* p,
                              suffix_context_t* context,
                              size_t str_len,
                              compare_fun_t compare,
                              sptr_t max_char, // the maximum character value..
                              // These args are here just for optimizing.
                              int bytes_per_character,
                              int bytes_per_pointer)
{
  unsigned char** L_buckets; // these are pointers into S.
  unsigned char** L_bucket_starts; // these are pointers into S.
  unsigned char** S_buckets;
  int n_buckets, n_buckets2;
  sptr_t tlast;
  error_t err;

  if( max_char <= 0 ) {
    n_buckets = 1 << (8*bytes_per_character);
  } else {
    n_buckets = max_char;
  }

  n_buckets2 = n_buckets * n_buckets;

  L_buckets = (unsigned char**) calloc(n_buckets, sizeof(unsigned char*));
  if( ! L_buckets ) return ERR_MEM;
  L_bucket_starts = (unsigned char**) calloc(n_buckets, sizeof(unsigned char*));
  if( ! L_bucket_starts ) return ERR_MEM;
  S_buckets = (unsigned char**) calloc(n_buckets2, sizeof(unsigned char*));
  if( ! S_buckets ) return ERR_MEM;
#define SL_BUCKET(c0, c1) (S_buckets[n_buckets * (c0) + (c1)])
  // if c0==c1 it must be an SS bucket (or a L bucket)
#define SS_BUCKET(c0, c1) (S_buckets[n_buckets * (c1) + (c0)]) 
#define L_BUCKET(c0) (L_buckets[(c0)]) 

  // now all buckets are initialized to 0.

#define CLASSIFY_SSL(SS_todo,SL_todo,L_todo) \
{ \
  sptr_t ti, tnext, tmid, tend, end; \
  /* tmid starts out as the first character. */ \
  tmid = get_T(p->T, bytes_per_character, 0); \
  /* find the smallest end>to such that ti_end!=ti_to. */ \
  /* Value in case we run out... end of the string is smaller than any other. */ \
  tend = -1; /* a character that won't actually occur..*/ \
  for( end = bytes_per_character; \
       end < p->t_size; \
       end += bytes_per_character ) { \
    tend = get_T(p->T, bytes_per_character, end); \
    if( tmid != tend ) break; \
  } \
  for( sptr_t mid, to = 0; \
       to < p->t_size; ) { \
    ti = tmid; \
    mid = end; \
    tmid = tend; \
    /* find the smallest end>mid such that t_end!+t_mid */ \
    /* Value in case we run out... end of the string is smaller than any other. */ \
    tend = -1; /* a character that won't actually occur..*/ \
    for( end = mid+bytes_per_character; \
         end < p->t_size; \
         end += bytes_per_character ) { \
      tend = get_T(p->T, bytes_per_character, end); \
      if( tmid != tend ) break; \
    } \
    if( ti < tmid ) { \
      /* suffixes to...mid are SX */ \
      if( tmid < tend ) { \
        /* suffixes to...mid are type SS */ \
        for( ; to < mid; to += bytes_per_character ) { \
          tnext = (to==mid-bytes_per_character)?(tmid):(ti); \
          SS_todo; \
        } \
      } else { \
        /* suffixes to...mid are type SL */ \
        for( ; to < mid; to += bytes_per_character ) { \
          tnext = (to==mid-bytes_per_character)?(tmid):(ti); \
          SL_todo; \
        } \
      } \
    } else  { /* ti > tmid */ \
      /* suffixes to...mid are type L */ \
      for( ; to < mid; to += bytes_per_character ) { \
        tnext = (to==mid-bytes_per_character)?(tmid):(ti); \
        L_todo; \
      } \
    } \
  } \
}
  // Partition into buckets.
  // First, count the number of bytes in each type
  CLASSIFY_SSL({
                 SS_BUCKET(ti,tnext) += bytes_per_pointer;
               },
               {
                 SL_BUCKET(ti,tnext) += bytes_per_pointer;
               },
               {
                 L_BUCKET(ti) += bytes_per_pointer;
               });

  // Next, turn the buckets into offsets in the input.
  // L_buckets point to the *end* of each bucket
  // SL_buckets point to the *end* of each bucket
  // SS_buckets point to the *end* of each bucket
  {
    unsigned char* next = p->S;
    sptr_t count;
    for( int j = 0; j < n_buckets; j++ ) {
      // first come the L-buckets.
      L_bucket_starts[j] = next;
      count = (sptr_t) L_BUCKET(j);
      next += count;
      L_BUCKET(j) = next;
      // now do all of the S-buckets...
      // if k == j it's an SS bucket or an L bucket.
      // because if k==j, k and j have the same type.
      count = (sptr_t) SS_BUCKET(j, j);
      next += count;
      SS_BUCKET(j, j) = next;
      for( int k = j+1; k < n_buckets; k++ ) {
        // for jk with j>k, start with SL buckets
        count = (sptr_t) SL_BUCKET(j, k);
        next += count;
        SL_BUCKET(j, k) = next;
        // now do SS buckets starting with jk
        count = (sptr_t) SS_BUCKET(j, k);
        next += count;
        SS_BUCKET(j, k) = next;
      }
    }
  }

  // Now sort the text into the buckets.
  CLASSIFY_SSL({
                 SS_BUCKET(ti,tnext) -= bytes_per_pointer;
                 set_S_with_ptr(bytes_per_pointer, SS_BUCKET(ti, tnext), to);
               },
               {
                 SL_BUCKET(ti,tnext) -= bytes_per_pointer;
                 set_S_with_ptr(bytes_per_pointer, SL_BUCKET(ti, tnext), to);
               },
               {
                 L_BUCKET(ti) -= bytes_per_pointer;
                 set_S_with_ptr(bytes_per_pointer, L_BUCKET(ti), to);
               });

  // Note that since CLASSIFY_SL goes forward, the last suffix
  // should be at the top of its L bucket, which is the correct position.
  tlast = get_T(p->T, bytes_per_character, p->t_size - bytes_per_character);

  // Now sort all of the SL-buckets.
  // buckets are currently pointing to the starts
  {
    string_sort_params_t local;

    local.context = context;
    local.base = NULL;
    local.n_memb = 0;
    local.memb_size = bytes_per_pointer;
    local.str_len = str_len;
    local.same_depth = bytes_per_character+bytes_per_character; // we've already sorted two chars.
    local.get_string = get_string_getter(context);
    local.compare = compare;

    // Go through the SL-buckets sorting them...
    for( int j = 0; j < n_buckets; j++ ) {
      for( int k = j+1; k < n_buckets; k++ ) {
        unsigned char* end;
        // the end of the SL-bucket is the SS-bucket following
        end = SS_BUCKET(j,k);

        // sort from SL_BUCKET(j,k) to end
        local.base = SL_BUCKET(j,k);
        local.n_memb = (end - SL_BUCKET(j,k))/bytes_per_pointer;
        if( local.n_memb > 1 ) {
          // don't bother calling sort for zero-sized or 1-sized buckets.
          err = favorite_string_sort(&local);
          if( err ) return err;
        }
      }
    }
  }

  // Change the bucket pointers for the SS buckets to point at the
  // end of the buckets. That's the next SL bucket or L bucket if 
  // there are no more SL buckets for those two characters.
  // Note that SS_BUCKET(n_buckets-1, n_buckets-1) must be empty
  // because it's the maximal character therefore it can't be an S suffix.
  for( int j = 0; j < n_buckets-1; j++ ) {
    for( int k = j; k < n_buckets-1; k++ ) {
      SS_BUCKET(j,k) = SL_BUCKET(j,k+1);
    }
    // the last SS bucket uses the next L bucket as its end.
    SS_BUCKET(j,n_buckets-1) = L_BUCKET(j+1);
  }

  // Also change the L-bucket for tlast to contain already the final
  // suffix as it's in the proper position.
  L_BUCKET(tlast) += bytes_per_pointer;

  // Now compute the proper ordering of the type SS buckets.
  // Scan SA from right to left and append to the ends of the SS-bucket
  // if SA[i]-1 is a SS suffix.
  // Note that the SL buckets will go above the SS-buckets.
  // Go through only the type S suffixes.
  // Again, there can be no S suffixes starting with the maximal character.
  for( int j = n_buckets-2; j >= 0; j-- ) {
    unsigned char* start = SL_BUCKET(j, j+1);
    unsigned char* end = L_bucket_starts[j+1];
    for( unsigned char* sp = end-bytes_per_pointer;
         sp >= start;
         sp -= bytes_per_pointer ) {
      sptr_t to, ti, tnext;
      to = get_S_with_ptr(bytes_per_pointer, sp);
      if( to == 0 ) continue; // text at 0 doesn't have a previous...
      tnext = j; // we're in S suffixes starting with j
      to -= bytes_per_character; // move to previous
      ti = get_T(p->T, bytes_per_character, to);
      // now, if ti <= tnext, SA[i]-1 is type SS.
      if( ti <= tnext ) {
        // to is a type S suffix and sp
        // it must be a type SS suffix.
        // copy to it to the END of the SS bucket.
        SS_BUCKET(ti, tnext) -= bytes_per_pointer;
        set_S_with_ptr(bytes_per_pointer, SS_BUCKET(ti, tnext), to);
      }
    }
  }


  // Now compute the proper ordering of the type L buckets
  // scan SA from left to right
  // if SA[i] - 1 is a type L suffix, put it at the start of the L-bucket.
  for( int j = 0; j < n_buckets; j++ ) {
    // go through the type-L suffixes with this start.
    // go through the type-S suffixes with this start.
    sptr_t to, ti, tnext;
    unsigned char* l_start = L_bucket_starts[j]; // need 2nd start array because we're moving L buckets.
    unsigned char* l_end = (j==n_buckets-1)?(p->S+p->s_size):(SL_BUCKET(j, j+1));
    unsigned char* s_end = (j==n_buckets-1)?(p->S+p->s_size):(L_bucket_starts[j+1]);
    unsigned char* sp;

    for( sp = l_start; sp < l_end; sp += bytes_per_pointer ) {
      // since suffix at sp is a L-type suffix, SA[i]-1 
      // will be type L if tnext <= ti
      to = get_S_with_ptr(bytes_per_pointer, sp);
      if( to == 0 ) continue;
      tnext = j;
      to -= bytes_per_character;
      ti = get_T(p->T, bytes_per_character, to);
      if( tnext <= ti ) {
        // SA[i]-1 is a type L suffix.
        // copy it to the top of the L-suffixes bin.
        set_S_with_ptr(bytes_per_pointer, L_BUCKET(ti), to);
        L_BUCKET(ti) += bytes_per_pointer;
      }
    }
    for( ; sp < s_end; sp += bytes_per_pointer ) {
      // since suffix at sp is a type S suffix, SA[i]-1
      // will be type L only if tnext < ti.
      to = get_S_with_ptr(bytes_per_pointer, sp);
      if( to == 0 ) continue;
      tnext = j;
      to -= bytes_per_character;
      ti = get_T(p->T, bytes_per_character, to);
      if( tnext < ti ) {
        // SA[i]-1 is a type L suffix.
        // copy it to the top of the L-suffixes bin.
        set_S_with_ptr(bytes_per_pointer, L_BUCKET(ti), to);
        L_BUCKET(ti) += bytes_per_pointer;
      }
    }
  }

  free(S_buckets);
  free(L_buckets);

  return ERR_NOERR;
}

error_t two_stage_double(suffix_sorting_problem_t* p,
                         suffix_context_t* context, size_t str_len, compare_fun_t compare, sptr_t max_char)
{
#define TSD(b_per_c, b_per_p, mc) if( p->bytes_per_character == b_per_c && p->bytes_per_pointer == b_per_p && max_char <= mc ) return two_stage_double_impl(p, context, str_len, compare, mc, b_per_c, b_per_p);

  if( p->bytes_per_character == 1 ) {
    // For 1 character, just always use 256 as max_char
    if( max_char <= 0 ) {
      TSD(1,1,256);
      TSD(1,2,256);
      TSD(1,3,256);
      TSD(1,4,256);
      TSD(1,5,256);
    }
    TSD(1,1,7); // to check boundary conditions on unit tests.
    TSD(1,1,128);
    TSD(1,1,256);
    TSD(1,2,128);
    TSD(1,2,256);
    TSD(1,3,128);
    TSD(1,3,256);
    TSD(1,4,128);
    TSD(1,4,256);
    TSD(1,5,128);
    TSD(1,5,256);
  } else {
    // two-byte characters means we actually 
    // have to use max_char...
    TSD(2,1,512);
    TSD(2,1,max_char);
  }

  TSD(p->bytes_per_character, p->bytes_per_pointer, max_char);
#undef TSD
}

/* Use two-stage to sort to the first str_len characters
and then do comparisons using the compare function.
str_len must be at least 3.
   */
error_t two_stage_ssort(suffix_sorting_problem_t* p,
                        suffix_context_t* context, size_t str_len, compare_fun_t compare, sptr_t max_char)
{
  error_t err;

  if( str_len < 3 ) return ERR_PARAM;
  if( p->bytes_per_character > 3 ) return ERR_PARAM;

  err = prepare_problem(p);
  if( err ) return err;

  // Allocate S.
  p->S = (unsigned char*) malloc(p->bytes_per_pointer*p->n);

  if( p->bytes_per_character >= 2 ) {
    // do the two-stage with single-characters
    return two_stage_single(p, context, str_len, compare, max_char);
  } else if( p->bytes_per_character == 1 ) {
    // do the improved two-stage with multiple characters
    return two_stage_double(p, context, str_len, compare, max_char);
    //return two_stage_double(p, context, str_len, compare, max_char);
  } else {
    // this should never be reached...
    return ERR_PARAM;
  }
}
