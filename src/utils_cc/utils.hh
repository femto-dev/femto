/*
  (*) 2008-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/utils.hh
*/
#ifndef _UTILS_HH_
#define _UTILS_HH_

#include <iostream>
#include <sstream>
#include <cstdlib>

#include <vector>
#include <string>
  
static inline
void* allocate_memory(size_t size)
{
  // operator new should throw an std::bad_alloc exception on failure
  // so we don't need to check the return value for NULL.
  return operator new(size);
}
static inline
void free_memory(void* ptr)
{
  operator delete(ptr);
}

#define PTR_ADD(void_star_ptr, offset) ((void*)(((char*)(void_star_ptr))+offset))
#define PTR_SUB(void_star_ptr, offset) ((void*)(((char*)(void_star_ptr))-offset))
#define PTR_DIFF(void_star_ptr_a, void_star_ptr_b) (((char*) (void_star_ptr_a)) - ((char*) (void_star_ptr_b)))

// Get the length of a file from a file descriptor.
off_t file_len(int fd);
// Get the length of a file from a FILE*
size_t file_len(FILE* f);
// Get the length of a file from a filename, or 
// throw an exception if it doesn't exist
off_t file_len(const std::string & filename);
void stat_file(const std::string & filename, bool& exists, bool& regular, off_t& len);

// Create a temporary file in tmp_dir.
FILE* tmp_file(std::string tmp_dir);

// Print a character to an ostringstream in a sensible way.
void print_character(std::ostringstream& os, unsigned long int ch);

// Print a character buffer to an ostringstream
// in a sensible way. This prints (len) abjcj\x00aba
template<typename character_t>
void print_len_and_buffer(std::ostringstream& os, size_t len, const character_t* data)
{
  os << std::dec << "(" << len << ")\"";
  for( size_t i = 0; i < len; i++ ) {
    print_character(os, data[i]);
  }
  os << "\"";
}

// Sets o_direct on a file if we can
// might fail and print an error. returns true if it's OK.
bool set_o_direct_fd(int fd, size_t transfer_size, const char* fname);
void clear_o_direct_fd(int fd, const char* fname);


void copy_file(const std::string & from_fname, const std::string & to_fname,
               bool move_not_copy=false);
void move_file(const std::string & from_fname, const std::string & to_fname);

void unlink_ifneeded(const std::string & fname);

void unlink_ifneeded(std::vector<std::string> fnames);

template<typename Num>
std::string num_to_string(Num x)
{
  std::ostringstream os;
  os << x;
  return os.str();
}

#endif
