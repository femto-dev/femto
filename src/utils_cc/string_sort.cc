extern "C" {
  #include "string_sort.h"
}
#include "varint.hh"
#include "bucket_sort.hh"

struct SortingProblemStringKey {
  typedef unsigned char character_t;

  unsigned char* str;
  size_t len;

  SortingProblemStringKey() : str(NULL), len(0) { }
  SortingProblemStringKey(unsigned char* data, size_t len) : str(data), len(len) { }

  typedef character_t key_part_t;
  key_part_t get_key_part(size_t i) const
  {
    return str[i];
  }
  size_t get_num_key_parts() const
  {
    return len;
  }
};

template <int PtrBytes>
struct SortingProblemFixedLengthStringRecordSortingCriterion {
  typedef return_key_criterion_tag criterion_category;
  typedef SortingProblemStringKey key_t;

  const string_sort_params_t *context;
  const get_string_fun_t get_str;
  const size_t len;

  SortingProblemFixedLengthStringRecordSortingCriterion( string_sort_params_t *context ) : context(context), get_str(context->get_string), len(context->str_len) { }

  key_t get_key(const be_uint<PtrBytes>& r)
  {
    SortingProblemStringKey ret(get_str(context, &r), len);
    return ret;
  }
};

template <int PtrBytes>
struct SortingProblemCompare {
  typedef compare_criterion_tag criterion_category;

  const string_sort_params_t *context;
  const compare_fun_t cmp;

  SortingProblemCompare( string_sort_params_t *context ) : context(context), cmp(context->compare) { }
  int compare(const be_uint<PtrBytes>& a, const be_uint<PtrBytes>& b) {
    return cmp(context, &a, &b);
  }
};

/*
    params.context = context;// context
    params.base = // base of pointer records
    params.n_memb = // how many pointer records
    params.memb_size ; // bytes per pointer record
    params.str_len ; // how many bytes to sort
    params.same_depth = 0; // not used here
    
    typedef string_t (*get_string_fun_t)(const void* context, const void* data_ptr);
    params.get_string = get_string_getter(context); // return a string
      in: context, pointer to record; out -> pointer to string bytes

      typedef int (*compare_fun_t)(const void* context, const void * a, const void * b);
     in: context, pointer to record, pointer to record; out -> cmp
    params.compare = compare;
*/


template<int PtrBytes>
void do_sorting_problem(string_sort_params_t *context)
{
  assert(context->memb_size == PtrBytes);
  
  SortingProblemFixedLengthStringRecordSortingCriterion<PtrBytes> cmp1(context);
  SortingProblemCompare<PtrBytes> cmp2(context);

  size_t len = context->str_len;
  unsigned char* minbuf = (unsigned char*) malloc(len);
  unsigned char* maxbuf = (unsigned char*) malloc(len);
  memset(minbuf, 0, len);
  memset(maxbuf, 0xff, len);

  SortingProblemStringKey min(minbuf, len);
  SortingProblemStringKey max(maxbuf, len);

  be_uint<PtrBytes>* records = (be_uint<PtrBytes>*) context->base;
  sort_array_compare_after(cmp1, cmp2, min, max, records, context->n_memb);

  free(minbuf);
  free(maxbuf);
}


error_t bucket_sort(string_sort_params_t* context)
{
  int ptrbytes = context->memb_size;
  if( ptrbytes == 1 ) do_sorting_problem<1>(context);
  else if( ptrbytes == 2 ) do_sorting_problem<2>(context);
  else if( ptrbytes == 3 ) do_sorting_problem<3>(context);
  else if( ptrbytes == 4 ) do_sorting_problem<4>(context);
  else if( ptrbytes == 5 ) do_sorting_problem<5>(context);
  else if( ptrbytes == 6 ) do_sorting_problem<6>(context);
  else if( ptrbytes == 7 ) do_sorting_problem<7>(context);
  else if( ptrbytes == 8 ) do_sorting_problem<8>(context);
  else return ERR_PARAM;

  return ERR_NOERR;
}

