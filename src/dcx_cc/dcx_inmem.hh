extern "C" {
  #include "error.h"
  #include "suffix_sort.h"
}

static const int DCX_FLAG_USE_TWO_STAGE = 2;
static const int DCX_FLAG_SOMETIMES_NAME = 4;

error_t dcx_inmem_ssort(suffix_sorting_problem_t* p, int period, int flags=0);

int dcx_inmem_supports_period(int period);

// returns the number of padding characters (you must
// multiply by bytes per characters) required for guaranteed
// sucessfull completion.
sptr_t dcx_inmem_get_padding_chars(int period);


