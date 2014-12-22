extern "C" {
  #include "error.h"
}


error_t two_stage_ssort(suffix_sorting_problem_t* p,
                        suffix_context_t* context, size_t str_len,
                        compare_fun_t compare, sptr_t max_char);

