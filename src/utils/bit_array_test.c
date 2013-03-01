/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/bit_array_test.c
*/
#include "bit_array.h"
#include <assert.h>

int main (int argc, char** argv)
{
  // Test #1 - allocate_bit_array
  //         - right_shift_one
  //         - get_bit
  uint64_t *bitArrayTest1 = allocate_bit_array(2);
  bit_array_right_shift_one(2, bitArrayTest1);
  bit_array_right_shift_one(2, bitArrayTest1);

  assert ( 1 == get_bit (2, bitArrayTest1, 0));
  assert ( 1 == get_bit (2, bitArrayTest1, 1));
  assert ( 0 == get_bit (2, bitArrayTest1, 2));
	   
  free(bitArrayTest1);

  printf("\nTest #1 passed\n");


  // Test #2 - allocate_bit_array (large!)
  //         - set_bit
  //         - get_bit
  uint64_t *bitArrayTest2 = allocate_bit_array(64);
  for (int i = 0; i < (64*64); i+=2)  {
    set_bit (64, bitArrayTest2, i);
    assert  ( 1 == get_bit (64, bitArrayTest2, i));
  }
      
  for (int i = 1; i < (64*64); i+=2) {
    assert ( 0 == get_bit (64, bitArrayTest2, i));
  }

  free(bitArrayTest2);
  
  printf("Test #2 passed\n");


  // Test #3 - allocate_bit_array
  //         - allocate_bit_array
  //         - set_bit (s)
  //         - bit_array_and
  
  
  // Test #4 - allocate_bit_array
  //         - allocate_bit_array
  //         - set_bit (s)
  //         - bit_array_or

  
  // Test #5 - allocate_bit_array
  //         - set_bit (s)
  //         - clear_bit (s)
  //         - bit_array_check_empty

  
  uint64_t *bitArrayTest5 = allocate_bit_array(1);
  for (int i = 0; i < (1*64); i+=2)  {
    set_bit (1, bitArrayTest5, i);
    assert ( 1 == get_bit (1, bitArrayTest5, i));
  }

  assert ( ! bit_array_check_empty (1, bitArrayTest5));
  
  for (int i = 0; i < (1*64); i+=2)  {
    clear_bit (1, bitArrayTest5, i);
  }

  assert ( bit_array_check_empty (1, bitArrayTest5));
  
  free(bitArrayTest5);

  printf("Test #5 passed\n");

  
    
  printf("\nAll the tests passed. \n\n");

  return 0;
}
