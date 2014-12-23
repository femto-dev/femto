#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

extern "C" {
  #include "error.h"
  #include "timing.h"
  #include "string_sort.h"
  #include "bit_funcs.h"
  #include "suffix_sort.h"
}

#include "dcover.hh" 
#include "dcx_inmem.hh"
#include "two_stage.hh"

//#define DEBUG 10
#define DEBUG 0
#define TIMING 0

static inline
error_t subproblem_sort(string_sort_params_t* params)
{
  return favorite_string_sort(params);
  //return multikey_qsort(params);
}

static inline
error_t suffix_sort_to_depth(suffix_sorting_problem_t* p,
                             int p_bytes_per_pointer,
                             suffix_context_t* context, size_t str_len, 
                             compare_fun_t compare,
                             sptr_t max_char,
                             int USE_TWO_STAGE)
{
  { // check that the context is O.K.
    assert(context);
    assert(context->T == p->T);
    assert(context->bytes_per_pointer == p_bytes_per_pointer);
  }
  if( USE_TWO_STAGE && p->bytes_per_character <= 3 && str_len >= 3) {
    return two_stage_ssort(p, context, str_len, compare, max_char);
  } else {
    // just do the sorting directly with our favorite string sorter
    string_sort_params_t params;
    int p_bytes_per_character = p->bytes_per_character;
    int p_n = p->n;
    unsigned char* p_S;

    // at this point, we must malloc the output S buffer.
    p->S = (unsigned char*) malloc(p_bytes_per_pointer * p->n);
    if ( ! p->S ) return ERR_MEM;
    p_S = p->S;

    // Create the initial values to sort...
    for( sptr_t i = 0, so = 0, to = 0;
        i < p_n;
        i++, so+=p_bytes_per_pointer, to+=p_bytes_per_character ) {
      set_S(p_bytes_per_pointer, p_S, so, to);
    }

    params.context = context;
    params.base = p->S;
    params.n_memb = p->n;
    params.memb_size = p_bytes_per_pointer;
    params.str_len = str_len;
    params.same_depth = 0;
    params.get_string = get_string_getter(context);
    params.compare = compare;
    return subproblem_sort(&params);
  }
}

// sample suffix passed in here is NOT a byte offset.
// returns an index into the sample ranks, NOT a byte offset
// this is like down_offset in dcx.hh
template<int Period>
static inline
sptr_t sample_suffix_to_sample_offset(sptr_t characters_per_mod,
                                      sptr_t sample_suffix)
{
  typedef Dcover<Period> cover_t;
  int mod = sample_suffix % cover_t::period;
  int offset = sample_suffix / cover_t::period;
  int pos = cover_t::g.get_sample(mod);
  if( DEBUG ) assert( pos >= 0 );
  sptr_t sample_idx = characters_per_mod * pos + offset;
  return sample_idx;
}

// stores the offsets of sample suffixes into S
template<int Period>
static inline
void build_sample_offsets(int bytes_per_character,
                          int bytes_per_pointer,
                          unsigned char* S,
                          sptr_t pointer_bytes_per_mod)
{
  typedef Dcover<Period> cover_t;
  // state used to fill the sample.
  int period_bytes = cover_t::period * bytes_per_character;
  sptr_t base = 0;
  for( int s = 0; s < cover_t::sample_size; s++, base+=pointer_bytes_per_mod ) {
    sptr_t off = cover_t::g.get_cover(s)*bytes_per_character;
    for( sptr_t i = 0;
        i < pointer_bytes_per_mod;
        i+=bytes_per_pointer, off+=period_bytes ) {
      // set sample.S
      set_S(bytes_per_pointer, S, base+i, off);
      if( DEBUG ) {
        sptr_t si = get_S(bytes_per_pointer, S, base+i);
        sptr_t test;
        assert(si == off);
        // test that sample_suffix_to_sample_offset doesn't fail.
        test = sample_suffix_to_sample_offset<Period>(
                       pointer_bytes_per_mod/bytes_per_pointer,
                       off/bytes_per_character );
        assert( test*bytes_per_pointer == base+i );
      }
    }
  }
}

// stores the sample characters in T
// in the usual order.
template<int Period>
static inline
void build_sample_characters(int sample_bytes_per_character,
                             unsigned char* sample_T,
                             int caller_bytes_per_character,
                             unsigned char* caller_T,
                             sptr_t characters_per_mod)
{
  typedef Dcover<Period> cover_t;
  int caller_period_bytes = cover_t::period * caller_bytes_per_character;
  sptr_t base = 0;
  sptr_t sample_character_bytes_per_mod = characters_per_mod*sample_bytes_per_character;
  for( int s = 0; s < cover_t::sample_size; s++, base+=sample_character_bytes_per_mod ) {
    sptr_t off = cover_t::g.get_cover(s)*caller_bytes_per_character;
    for( sptr_t i = 0;
        i < sample_character_bytes_per_mod;
        i+=sample_character_bytes_per_mod, off+=caller_period_bytes ) {
      // set sample.S
      memcpy(sample_T+base+i,
             caller_T+off,
             sample_bytes_per_character);
    }
  }
}

// Returns the rank of suffix... when the cover is used
// to create the sample..
// Input and output are INDEX, not byte offsets.
template<int Period>
static inline
sptr_t rank_of_sample_suffix(unsigned char* sampleR,
                             int sampleR_bytes_per_rank,
                             sptr_t characters_per_mod,
                             sptr_t at)
{
  //typedef Dcover<Period> cover_t;
  // so supposedly at is a sample suffix.
  sptr_t sample_idx = sample_suffix_to_sample_offset<Period>(characters_per_mod, at);

  return get_be(sampleR + sample_idx*sampleR_bytes_per_rank, sampleR_bytes_per_rank);
}

typedef struct sorting_state_s {
  suffix_context_t ctx;
  size_t p_bytes_per_character;
  //dcover_t cover;
  unsigned char* sampleR;
  size_t sampleR_bytes_per_rank;
  sptr_t characters_per_mod;
} sorting_state_t;

// The compare_with_sample_ranks could be specialized by
// inline by p_bytes_per_pointer, p_bytes_per_character, sampleR_bytes_per_rank, cover_period
template<int Period>
static inline
int compare_with_sample_ranks_core(int p_bytes_per_pointer,
                                   int p_bytes_per_character,
                                   int sampleR_bytes_per_rank,
                                   const void* context, const void* av, const void* bv)
{
  typedef Dcover<Period> cover_t;
  sorting_state_t* state = (sorting_state_t*) context;
  sptr_t sa, sb;
  int a_amt, b_amt;
 
  sa = get_S_with_ptr(p_bytes_per_pointer, (unsigned char*) av);
  sb = get_S_with_ptr(p_bytes_per_pointer, (unsigned char*) bv);
  // support for bit-flipping done by two-stage.
  sa = (sa<0)?(~sa):(sa);
  sb = (sb<0)?(~sb):(sb);

  sa /= p_bytes_per_character;
  sb /= p_bytes_per_character;

  cover_t::g.which_offsets_to_use(sa % cover_t::period,
                                  sb % cover_t::period,
                                  &a_amt, &b_amt);

  // return the comparison of S[sa+a_amt] vs S[sb+b_amt].

  // now get the ranks for each suffix from the sample.
  sptr_t ranka = rank_of_sample_suffix<Period>( state->sampleR,
                                       sampleR_bytes_per_rank,
                                       state->characters_per_mod,
                                       sa+a_amt);
  sptr_t rankb = rank_of_sample_suffix<Period>( state->sampleR,
                                       sampleR_bytes_per_rank,
                                       state->characters_per_mod,
                                       sb+b_amt);

  ranka = ranka - rankb;
  if( ranka < 0 ) return -1;
  else if ( ranka > 0 ) return 1;
  else return 0;
}

#define NAME_CWSR(a, b, c) \
  compare_with_sample_ranks_ ## a ## _ ## b ## _ ## c

#define DECLARE_CWSR(a, b, c) \
template<int Period> \
static \
int compare_with_sample_ranks_ ## a ## _ ## b ## _ ## c (const void* context, const void* av, const void* bv) { \
  return compare_with_sample_ranks_core<Period>(a, b, c, context, av, bv); \
}

#define GET_CWSR(a, b, c, Period) \
  if( state->ctx.bytes_per_pointer == a &&  \
      state->p_bytes_per_character == b &&  \
      state->sampleR_bytes_per_rank == c ) { \
    return & NAME_CWSR(a, b, c)<Period>; \
  }

DECLARE_CWSR(4, 4, 4);
DECLARE_CWSR(4, 3, 4);
DECLARE_CWSR(4, 1, 4);
DECLARE_CWSR(4, 3, 2);
DECLARE_CWSR(1, 1, 1);

template<int Period>
static
int compare_with_sample_ranks_x_x_x_x(const void* context, const void* av, const void* bv) {
  sorting_state_t* state = (sorting_state_t*) context;
  return compare_with_sample_ranks_core<Period>(state->ctx.bytes_per_pointer,
                                        state->p_bytes_per_character,
                                        state->sampleR_bytes_per_rank,
                                        context, av, bv);
}

template<int Period>
static
compare_fun_t get_compare_with_sample_ranks(sorting_state_t* state)
{
  GET_CWSR(4, 4, 4, Period);
  GET_CWSR(4, 3, 4, Period);
  GET_CWSR(4, 1, 4, Period);
  GET_CWSR(4, 3, 2, Period);
  GET_CWSR(1, 1, 1, Period);

  printf("Perf Note: compare_with_sample_ranks_%i_%i_%i_%i does not exist; using default\n", 
      (int) state->ctx.bytes_per_pointer,
      (int) state->p_bytes_per_character,
      (int) state->sampleR_bytes_per_rank,
      (int) Period);

  return & compare_with_sample_ranks_x_x_x_x<Period>;
}

// cover is a difference cover to use for this problem.
// Input: p->T
// max_char is 1+ the maximum attainable character value
// (e.g 256 for bytes) or <=0 if it should be based on the # of bytes only.
// Output: p->S (allocated by this routine)
template<int Period>
error_t dcx_ssort_core(suffix_sorting_problem_t* p, sptr_t max_char,
                       int flags)
{
  typedef Dcover<Period> cover_t;
  suffix_sorting_problem_t sample;
  // sample ranks.
  unsigned char* sampleR;
  size_t sampleR_bytes_per_rank;
  sptr_t characters_per_mod;
  sptr_t num_ranks;
  error_t err;

  // values which are copies...
  size_t p_bytes_per_pointer;
  size_t p_bytes_per_character;
  int cover_period;

  // use the S/L two-stage suffix sorter to do the whole subproblem
  // sort (if it can - depending on the character size).
  int USE_TWO_STAGE = (flags & DCX_FLAG_USE_TWO_STAGE) > 0;
  // sometimes name improvement - we can just copy text 
  // instead of renaming if we don't expect any savings
  // (e.g. we're creating 3-byte 'characters' and pointers are 4 bytes)
  int SOMETIMES_NAME = (flags & DCX_FLAG_SOMETIMES_NAME) > 0;

  if( DEBUG > 3 ) printf("dcx_ssort\n");

  err = prepare_problem(p);
  if( err ) return err;

  p_bytes_per_pointer = p->bytes_per_pointer;
  p_bytes_per_character = p->bytes_per_character;
  cover_period = cover_t::period;

  // figure out how big the sample will be

  // Include at least one NULL character after each set;
  // in other words, pretend that there are cover.period NULLs
  // at the end of the input string; and we also add a character to make
  // sure that we have a 0 character between each subproblem in recursion.
  characters_per_mod = 1 + ceildiv(p->n, cover_period);

  sample.n = cover_t::sample_size * characters_per_mod;

  // now, if the number of characters is less than the 
  // cover period, or sample.n is not less than n,
  // we just sort the strings using suffix_sort_to_depth.
  if( p->n <= cover_period ||
      sample.n >= p->n ) {
    if( DEBUG ) {
      printf("Starting basic suffix sort for small subproblem %li\n", p->n);
    }
    suffix_context_t context;
    sptr_t str_len;

    // how many bytes per string do we need to sort?
    str_len = p->n * p_bytes_per_character;
    // There should be padding enough to not need special
    // cases for the last suffix (at p->n-1).
    assert( str_len < p->t_padding_size );

    context.T = p->T;
    context.bytes_per_pointer = p_bytes_per_pointer;
    // sort all of the suffixes in S.
    return suffix_sort_to_depth(p, p_bytes_per_pointer,
                                &context, str_len, NULL, max_char, USE_TWO_STAGE);
  }

  if( TIMING ) {
    printf("Starting dcx_ssort for %li characters\n", p->n);
    start_clock();
  }

/*
  // refine sample_n
  {
    // now compute the proper sample_n by using the maximum value
    // in the sub-problems. We're doing this so that we can avoid
    // having zeros padding the end of the string, since repeated
    // zeros at the end become nonunique and then the algorithm
    // doesn't terminate.
    //printf("sample_n %i\n", (int) sample_n);
    sptr_t biggest_sub = 0;
    sptr_t start = 0;
    if( p->n > cover_t::period ) start = p->n - cover_t::period;

    for( sptr_t i = start; i < p->n; i++ ) {
      if( cover_t::g.in_cover(i % cover_t::period) ) {
        sptr_t sub_offset = sample_suffix_to_sample_offset<Period>(characters_per_mod,i);
        if( sub_offset > biggest_sub ) biggest_sub = sub_offset;
      }
    }

    //printf("biggest_sub %i\n", (int) biggest_sub);
    // this should just be making a tighter bound.
    assert( biggest_sub < sample.n );
    sample.n = biggest_sub + 1;

    if( p->n == 0 ) sample.n = 0;
  }
*/

  // step 0: construct a sample
  sample.bytes_per_character = 0;
  // sample elements will be pointers into orignal T.
  sample.bytes_per_pointer = p_bytes_per_pointer;
  sample.T = NULL;
  sample.S = NULL;
  sampleR = NULL;
  sampleR_bytes_per_rank = 0;

  // if we need to, rename characters with ranks.
  if( !SOMETIMES_NAME ||
      // the first part of this one is here to prevent overflow.
      p->bytes_per_character*cover_period >= (int) sizeof(sptr_t)-1 || 
      (sptr_t) pow2i(8*p->bytes_per_character*cover_period) > sample.n ) { // alphabet size > input size
    sptr_t depth_bytes;

    // create room for the sample offsets
    sample.S = (unsigned char*) malloc(sample.n*sample.bytes_per_pointer);
    if( ! sample.S ) {
      err = ERR_MEM;
      goto error;
    }

    // build the sample 
    // Note that the offsets in the sample are 
    // offsets into p->T (and not sample->T,
    // which doesn't currently exist)
    build_sample_offsets<Period>(p->bytes_per_character,
                         sample.bytes_per_pointer,
                         sample.S,
                         characters_per_mod*sample.bytes_per_pointer);

    depth_bytes = p->bytes_per_character * cover_period;

    // sort the sample by the first cover->period characters
    {
      string_sort_params_t params;
      suffix_context_t context;

      context.T = p->T;
      context.bytes_per_pointer = sample.bytes_per_pointer;

      params.context = &context;
      params.base = sample.S;
      params.n_memb = sample.n; 
      params.memb_size = sample.bytes_per_pointer;
      params.str_len = depth_bytes;
      params.same_depth = 0;
      params.get_string = get_string_getter(&context);
      params.compare = NULL;
      if( TIMING ) start_clock();
      err = subproblem_sort(&params);
      if( err ) return err;
      if( TIMING ) stop_clock();
      if( TIMING ) print_timings("first subproblem sort", p->n);

      if( DEBUG > 10 ) {
        printf("After first subproblem sort:\n");
        print_strings(&params, params.base, params.n_memb);
      }
    }

    num_ranks = -1;
    // compute the ranks of the characters in S; store those in R.
    // This could compute the inverse of S in-place...
    // but it's not a sure thing that would be better; we'll
    // certainly use more memory than that later...
    {
      unsigned char* last;
      sptr_t rank;
      sptr_t R_size;
      sptr_t R_pad;

      sampleR_bytes_per_rank = pointer_bytes_needed_for(sample.n);
      R_size = sample.n*sampleR_bytes_per_rank;
      R_pad = dcx_inmem_get_padding_chars(Period)*sampleR_bytes_per_rank;

      sample.t_padding_size = R_pad;

      sampleR = (unsigned char*) calloc(R_size+R_pad, 1);
      if( ! sampleR ) {
        err = ERR_MEM;
        goto error;
      }

      // do the naming.
      last = NULL;
      rank = -1;
      for( sptr_t i = 0, so = 0; i < sample.n; i++, so+=sample.bytes_per_pointer ) {
        sptr_t si = get_S(sample.bytes_per_pointer,
                          sample.S,
                          so);
        sptr_t sample_idx;
        unsigned char* key = p->T+si;
        if( ! last || memcmp(key, last, depth_bytes) ) {
          // key is different from last.
          rank++;
        }
        // change to sample indexing
        sample_idx = sample_suffix_to_sample_offset<Period>(
                                                    characters_per_mod,
                                                    si/p->bytes_per_character);
        set_be(sampleR+(sample_idx*sampleR_bytes_per_rank),
               sampleR_bytes_per_rank,
               rank);
        last = key;
      }
      num_ranks = rank + 1;
    }
    // free the S we allocated.
    free(sample.S);
    sample.S = NULL;

    // allocate sample.T which will store characters that 
    // are only as large as they need to be -- there
    // could be quite a few duplicates among the sampleR ranks.
    // The idea is to reduce the recursive memory cost and
    // to reduce the amount of sorting in recursive subproblems.
    sample.bytes_per_character = pointer_bytes_needed_for(num_ranks);
    if( sample.bytes_per_character == sample.bytes_per_pointer) {
      // just copy the pointer of R to T.
      sample.T = sampleR;
    } else {
      sptr_t T_size;
      sptr_t T_pad;

      // copy (and compress) the ranks
      T_size = sample.n*sample.bytes_per_character;
      T_pad = dcx_inmem_get_padding_chars(Period)*sample.bytes_per_character;

      sample.t_padding_size = T_pad;
      sample.T = (unsigned char*) calloc(T_size+T_pad, 1);
      if( ! sample.T ) {
        err = ERR_MEM;
        goto error;
      }

      // Set our "characters" which are actually just ranks.
      for( sptr_t i = 0, ro = 0, to = 0;
           i < sample.n;
           i++, ro+=sampleR_bytes_per_rank, to+=sample.bytes_per_character ) {
        sptr_t rank;
        rank = get_be(sampleR+ro, sampleR_bytes_per_rank);
        set_T(sample.T, sample.bytes_per_character, to, rank);
      }
    }
  } else {
    sptr_t T_size;
    sptr_t T_pad;

    if( DEBUG > 3 ) printf("dcx_ssort copying characters\n");
    // Otherwise, just copy the new characters.
    sample.bytes_per_character = p->bytes_per_character * cover_period;

    T_size = sample.n*sample.bytes_per_character;
    T_pad = dcx_inmem_get_padding_chars(Period)*sample.bytes_per_character;

    sample.t_padding_size = T_pad;
    sample.T = (unsigned char*) calloc(T_size+T_pad, 1);
    if( ! sample.T ) {
      err = ERR_MEM;
      goto error;
    }

    // store the characters in the sample..
    build_sample_characters<Period>(sample.bytes_per_character,
                            sample.T,
                            p->bytes_per_character,
                            p->T,
                            characters_per_mod);
    num_ranks = -1;
  }

  if( num_ranks == sample.n ) {
    // all the ranks are different -
    // we don't need to recurse.
    // just keep the sample.R we computed.
    // free sample.T.
    if( sampleR != sample.T ) free(sample.T);
    sample.T = NULL;
  } else {
    // free up the sample.R we allocated.
    if( sampleR != sample.T ) free(sampleR);
    sampleR = NULL;

    // step 1: sort sample suffixes
    // make the recursive call.
    err = dcx_ssort_core<Period>(&sample, num_ranks, flags);
    if( err ) {
      goto error;
    }

    // we don't need the sample text anymore - just the suffix array...
    free(sample.T);
    sample.T = NULL;

    sampleR_bytes_per_rank = sample.bytes_per_pointer;

    // allocate new ranks for the sample.
    sampleR = (unsigned char*) malloc(sample.n * sampleR_bytes_per_rank);
    if( ! sampleR ) {
      err = ERR_MEM;
      goto error;
    }

    // compute the ranks for the sample
    // by inverting the S we computed.
    for( sptr_t i = 0, so = 0;
        i < sample.n;
        i++, so += sample.bytes_per_pointer ) {
      // offset is a byte offset into the text...
      sptr_t offset = get_S(sample.bytes_per_pointer,
                            sample.S, 
                            so);
      // turn offset into an index from a byte offset into T
      offset /= sample.bytes_per_character;
      // turn that index into the appropriate rank.
      offset *= sampleR_bytes_per_rank;
      // set the rank at the correct offset
      set_be(sampleR+offset,
             sampleR_bytes_per_rank,
             i);
    }

    // we no longer need the sample S array.
    free(sample.S);
    sample.S = NULL;
  }

  if( TIMING ) stop_clock();
  if( TIMING ) print_timings("dcx_ssort create sample", p->n);


  // step 2: sort all suffixes in S.
  // now sort all of the suffixes in S.
  // Here we can use a suffix-sorter-to-depth.
  {
    sorting_state_t state;

    state.ctx.T = p->T;
    state.ctx.bytes_per_pointer = p_bytes_per_pointer;
    state.p_bytes_per_character = p_bytes_per_character;
    state.sampleR = sampleR;
    state.sampleR_bytes_per_rank = sampleR_bytes_per_rank;
    state.characters_per_mod = characters_per_mod;

    if( TIMING ) start_clock();
    err = suffix_sort_to_depth(p, p_bytes_per_pointer,
                               (suffix_context_t*) &state,
                               p->bytes_per_character * cover_period,
                               get_compare_with_sample_ranks<Period>(&state),
                               max_char, USE_TWO_STAGE);

    if( err ) return err;
    if( TIMING ) stop_clock();
    if( TIMING ) print_timings("second subproblem sort", p->n);
    if( DEBUG > 10 ) {
      printf("After second subproblem sort:\n");
      print_suffixes(p, Period);
    }
  }

  // free the last remaining part of the sample..
  free(sampleR);
  sampleR = NULL;

  return ERR_NOERR;

  error:
    free(sample.T);
    sample.T = NULL;
    free(sample.S);
    sample.S = NULL;
    free(sampleR);
    sampleR = NULL;
    free(p->S);
    p->S = NULL;

    return err;
}

error_t dcx_inmem_ssort(suffix_sorting_problem_t* p, int period, int flags)
{
  if( period == 3 ) {
    return dcx_ssort_core<3>(p, 0, flags);
  } else if( period == 7 ) {
    return dcx_ssort_core<7>(p, 0, flags);
  } else if( period == 13 ) {
    return dcx_ssort_core<13>(p, 0, flags);
  } else if( period == 95 ) {
    return dcx_ssort_core<95>(p, 0, flags);
  } else if( period == 133 ) {
    return dcx_ssort_core<133>(p, 0, flags);
/*  } else if( period == 4096 ) {
    return dcx_ssort_core<4096>(p, 0, flags);
  } else if( period == 8192 ) {
    return dcx_ssort_core<8192>(p, 0, flags);
    */
  } else {
    assert(0 && "dcx_ssort cover period not supported");
    return ERR_PARAM;
  }
}

int dcx_inmem_supports_period(int period)
{
  if( period == 3 ) return 1;
  else if( period == 7 ) return 1;
  else if( period == 13 ) return 1;
  else if( period == 95 ) return 1;
  else if( period == 133 ) return 1;
  return 0;
}

sptr_t dcx_inmem_get_padding_chars(int period)
{
  // This must return at least 12 for DC3
  // the formula is
  // 2vp/(p-v) >= required padding; where v is the sample size and p is the period.
  // For everything beyond DC3, v <= p/2,
  // and so the formula simplifies to 2p >= padding
  if( period == 3 ) return 12;
  else return 2*period;
}


