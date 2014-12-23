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

  femto/src/utils/util.c
*/
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
// for stat
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

#include "util.h"
#include "bswap.h"

// append newValue to the end of the array, updating count and array (it might be realloced)
// creates a new array if the array is empty.
// returs 0 for no error.
error_t append_array_internal(int* count, void** array, size_t member_size, void* newValue)
{
   unsigned char* a = *array;
   int c = *count;
   int exp;

   if( 0 == c ) exp = 0;
   else exp = 1 << log2i(c);
   if( c == exp ) {
      // the array is full - double its size.
      if( exp == 0 ) exp = 1;
      else exp = 2*exp;

      a = (unsigned char*) realloc(a, exp*member_size);
      if( !a ) return ERR_MEM;
      *array = a;
   }
   // now put the new value in.
   memcpy(a + c*member_size, newValue, member_size );
   *count = c+1; // increment the count.

   return ERR_NOERR;
}

int compare_int64(const void* aP, const void* bP)
{
  int64_t a = * (int64_t*) aP;
  int64_t b = * (int64_t*) bP;

  if( a < b ) return -1;
  else if ( a == b ) return 0;
  else return 1;
}

int compare_uint64(const void* ap, const void* bp)
{
  uint64_t a = * (const uint64_t *) ap;
  uint64_t b = * (const uint64_t *) bp;

  if( a < b ) return -1;
  else if( a > b ) return 1;
  else return 0;
}

// returns the new length of the array.
size_t sort_dedup_free(void* baseIn, size_t nmemb, size_t size,
                  int(*compar)(const void*, const void*),
                       void(*freef)(void*))
{
  unsigned int i;
  unsigned char* base = (unsigned char*) baseIn;

  // sort offsets.
  qsort( base, nmemb, size, compar);

  // remove duplicates in-place.
  i = 0; // i is the output index.
  for( unsigned int k = 0; k < nmemb; k++ ) {
    // if we're not at the first one, and this one is the
    // same as the last one, skip it.
    if( i != 0 && 0 == compar(base+size*k,base+size*(i-1)) ) {
      if( freef ) freef(base+size*k);
      continue;
    }
    if( i == k ) {
      i++; // "copy"
      continue; // don't need to copy - already there.
    }
    // otherwise, we must copy and increment i.
    memcpy(base+size*i, base+size*k, size);
    i++;
  }

  return i;
}

size_t sort_dedup(void* baseIn, size_t nmemb, size_t size,
                  int(*compar)(const void*, const void*))
{
  return sort_dedup_free(baseIn, nmemb, size, compar, NULL);
}

void reverse_array(void* array, size_t nmemb, size_t size)
{
  void* temp = malloc(size);
  for( size_t i = 0; i < nmemb/2; i++ ) {
    // swap array[i] and array[nmemb - i - 1]
    // temp = array[i]
    memcpy( temp,
            ((unsigned char*)array) + i*size,
            size);
    // array[i] = array[nmemb - i - 1]
    memcpy( ((unsigned char*)array) + i*size,
            ((unsigned char*)array) + (nmemb - i - 1)*size,
            size);
    // array[nmemb - i - 1] = temp
    memcpy( ((unsigned char*)array) + (nmemb - i - 1)*size,
            temp,
            size);
  }
  free(temp);
}

error_t write_int32(FILE* f, uint32_t x)
{
  int wrote;
  uint32_t temp;

  temp = hton_32(x);
  wrote = fwrite(&temp, 4, 1, f);
  if( wrote != 1 ) return ERR_IO_UNK;

  return ERR_NOERR;
}
error_t write_int64(FILE* f, uint64_t x)
{
  int wrote;
  uint64_t temp;

  temp = hton_64(x);
  wrote = fwrite(&temp, 8, 1, f);
  if( wrote != 1 ) return ERR_IO_UNK;

  return ERR_NOERR;

}
error_t write_int8(FILE* f, unsigned char x)
{
  int wrote;

  wrote = fwrite(&x, 1, 1, f);
  if( wrote != 1 ) return ERR_IO_UNK;

  return ERR_NOERR;
}



error_t write_string(FILE* f, char* s)
{
  int wrote;

  wrote = fwrite(s, strlen(s)+1, 1, f);
  if( wrote != 1 ) return ERR_IO_UNK;

  return ERR_NOERR;
}

error_t pad_to_align(FILE* f)
{
  int wrote;
  unsigned char zero = 0;

  while( ftell(f) & ALIGN_MASK ) {
    wrote = fwrite(&zero, 1, 1, f);
    if( wrote != 1 ) return ERR_IO_UNK;
  }

  return ERR_NOERR;
}


error_t mkdir_if_needed(const char* path)
{
  struct stat stats;
  int err;

  err = stat(path, &stats);
  if( err ) {
    if( errno == ENOENT ) { 
      // Try making the directory and running stat again.
      err = mkdir(path, 0777);
      if( err && errno != EEXIST) {
        return ERR_IO_STR_OBJ("Could not mkdir", path);
      }
      // Try statting again to make sure we got it.
      err = stat(path, &stats);
    }

    // If we havn't fixed the error by now, bail!
    if( err ) return ERR_IO_STR_OBJ("Could not stat", path);
  }

  if( ! S_ISDIR(stats.st_mode) ) return ERR_IO_STR_OBJ("Not a dir", path);

  // By now, path should exist and be a dir.
  return ERR_NOERR;
}



struct unit_info {
  char* name;
  double num;
};

struct unit_info u_info[] = {
  {"", 1L},
  {"Ki", 1024L},
  {"Mi", 1024L*1024L},
  {"Gi", 1024L*1024L*1024L},
  {"Ti", 1024LL*1024LL*1024LL*1024LL},
  {NULL,0}
};

struct val_unit get_unit(double value)
{
  struct val_unit ret;
  int i;
  int neg=0;

  if( value < 0 ) {
    neg = 1;
    value = - value;
  }

  // We can always use bytes.
  ret.value = value;
  ret.unit = 0;
  if( value < u_info[0].num ) return ret;

  for( i = 0; u_info[i].name && value >= u_info[i].num; i++ ) {
  }

  i--;

  ret.unit = i;
  ret.value = value;
  ret.value /= u_info[i].num;
  if( neg ) ret.value = - ret.value;
  return ret;
}

char* get_unit_prefix(struct val_unit x)
{
  assert(0<=x.unit && x.unit < sizeof(u_info)/sizeof(struct unit_info));
  return u_info[x.unit].name;
}

error_t resize_file(int fd, off_t length)
{
  unsigned char zero;
  ssize_t written;
  off_t at;
  int rc;

  at = lseek(fd, 0, SEEK_END);
  if( at == (off_t) -1 ) return ERR_IO_STR_NUM("lseek failed", errno);

  if( length > at && length > 0 ) {
    // extend the file to at least length bytes.
    at = lseek(fd, length - 1, SEEK_SET);
    if( at == (off_t) -1 ) return ERR_IO_STR_NUM("lseek failed", errno);
    written = write(fd, &zero, 1);
    if( written != 1 ) return ERR_IO_STR_NUM("write failed", errno);
    // seek back to where we were.
    at = lseek(fd, at, SEEK_SET);
    if( at == (off_t) -1 ) return ERR_IO_STR_NUM("lseek failed", errno);
  }

  // truncate the file.
  rc = ftruncate(fd, length);
  if( rc ) return ERR_IO_STR_NUM("ftruncate failed", errno);

  // make sure that we ended up with a file of the right size!
  at = lseek(fd, 0, SEEK_END);
  if( at == (off_t) -1 ) return ERR_IO_STR_NUM("lseek failed", errno);

  if( at != length ) return ERR_IO_STR("resize_file failed");

  return ERR_NOERR;
}

/* Finds and returns the integer index i
   such that 
   ntoh_32(arr[i]) <= target < ntoh_32(arr[i+1]).
   May return -1 for i; in that case, target < ntoh(arr[0]).
   May return n-1 for i; in that case, ntoh_32(arr[n-1]) <= target.
   Assumes that arr is a sorted list of numbers in network byte order.
*/
long bsearch_uint32_ntoh_arr(long n, uint32_t* arr, uint32_t target)
{
  long a, b, middle;

  if( target < ntoh_32(arr[0]) ) return -1;
  if( ntoh_32(arr[n-1]) <= target ) return n-1;

  a = 0;
  b = n - 1;
  // always we have that arr[a] <= target < arr[b].

  // divide the search space in half
  while(b - a > 1) {
    middle = (a + b) / 2;
    if( target < ntoh_32(arr[middle]) ) b = middle;
    else a = middle; // arr[middle] <= target
  }

  return a;
}

/* Finds and returns the integer index i
   such that 
   ntoh_64(arr[i]) <= target < ntoh_64(arr[i+1]).
   May return -1 for i; in that case, target < ntoh(arr[0]).
   May return n-1 for i; in that case, ntoh_64(arr[n-1]) <= target.
   Assumes that arr is a sorted list of numbers in network byte order.
*/
long bsearch_int64_ntoh_arr(long n, int64_t* arr, int64_t target)
{
  long a, b, middle;

  if( target < ntoh_64(arr[0]) ) return -1;
  if( ntoh_64(arr[n-1]) <= target ) return n-1;

  a = 0;
  b = n - 1;
  // always we have that arr[a] <= target < arr[b].

  // divide the search space in half
  while(b - a > 1) {
    middle = (a + b) / 2;
    if( target < ntoh_64(arr[middle]) ) b = middle;
    else a = middle; // arr[middle] <= target
  }

  return a;
}

uint64_t gcd64(uint64_t a, uint64_t b)
{
  uint64_t m;

  // Always have a > b.
  if( b > a ) {
    m = a;
    a = b;
    b = m;
  }

  while( b != 0 ) {
    m = a % b;
    a = b;
    b = m;
  }

  return a;
}

uint64_t lcm64(uint64_t a, uint64_t b)
{
  uint64_t ret = (a*b)/gcd64(a,b);
  if( EXTRA_CHECKS ) {
    assert( (ret % a) == 0 );
    assert( (ret % b) == 0 );
  }
  return ret;
}

#ifdef HAS_RECIPROCAL_DIVIDE

uint64_recip_t compute_reciprocal(uint64_t d)
{
  // See Division by Invariant Integers using Multiplication
  // by Granlund and Montgomery

  // compute L == ciel(log2(d))
  int log = log2lli(d); // this is lower bound of log2lli(d)
  int L = log + (d > pow2i(log));

  uint64_t q, r;
  uint64_recip_t ret;

  // sh1 = min(l,1)
  ret.sh1 = (L<1)?(L):(1);
  // sh2 = max(l-1,0)
  ret.sh2 = (L-1>0)?(L-1):(0);

  // compute m' = floor((2^n(2^l-d))/d) + 1
  div128_64(pow2i(L) - d, 0, d, q, r);

  ret.mprime = q + 1;
  return ret;
}
#endif


