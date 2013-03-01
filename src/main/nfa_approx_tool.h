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

  femto/src/main/nfa_approx_tool.h
*/
#ifndef _NFA_APPROX_TOOl_H_
#define _NFA_APPROX_TOOL_H 1

typedef struct {
  //array used to keep track of the different characters in the text
  uint64_t *alpha_array;
  //array of the sequential jumps that are not (only) sequential
  uint64_t *non_jumps;
  //array used as the bit array for a character in the text not in the NFA
  uint64_t *empty;
  //array used to redirect the program to the appropriate character array
  alpha_t  *redirect_array;
  //array of the bits arrays for each character in the NFA
  uint64_t **character_arrays;
  //array of the state arrays for different numbers of errors
  uint64_t **error_arrays;
  //array of arrays for R[d-1][j-1] statements
  uint64_t **previous_arrays;
} nfa_state;

typedef struct {
  int num_errors;
  int offset;
} match;


void create_character_node  (thompson_nfa_description_t *NFA,
			     int node_num, int ch);
void create_epsilon_node    (thompson_nfa_description_t *NFA,
			     int node_num, int args, ...);
void do_epsilon_transitions (thompson_nfa_description_t *NFA,
			     uint64_t *array, int number);
int init_nfa_state         (thompson_nfa_description_t* nfa,
			     nfa_state* state, int errors);


int make_matches (thompson_nfa_description_t *NFA, int input_len,
		   unsigned char* input, nfa_state* state,
		   int num_matches, match* matches, int errors);

void free_nfa_state (nfa_state* state, int counter, int errors);

int check_args (int argc, char** argv);

#endif
