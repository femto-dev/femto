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

  femto/src/main/nfa.h
*/
#ifndef _NFA_H_
#define _NFA_H_ 1

/** This file describes two kinds of NFAs - Thompson NFAs
    as well as generic NFAs. A Thompson NFA has epsilon nodes
    and character nodes. Character nodes have a single character transition
    which always goes to the node with the next number. Epsilon nodes
    can have as many transitions as they want, but the cannot have
    character transitions.
*/
#include <stdio.h>
#include "index_types.h"
#include "error.h"

#define CHARACTER_SET_CHUNKS (ALPHA_SIZE_DIV64)

typedef struct {
  // if there's a character transition, it always
  // goes to node_number+1
  alpha_t character;
  // a character_set transition allows the characters with 1s in the
  // corresponding place in this bit mask... or NULL if no character set.
  // ALPHA_SIZE bits in CHARACTER_SET_CHUNKS chunks
  uint64_t* character_set;

  // epsilon transitions going OUT of this node
  int num_epsilon;
  int* epsilon_dst;
} thompson_nfa_node_t;

// the start state is always 0 
// the accept state is always num_nodes - 1
typedef struct {
  int num_nodes;
  thompson_nfa_node_t* nodes;
} thompson_nfa_description_t;

void print_thompson_nfa(FILE* f, thompson_nfa_description_t* nfa);
void free_thompson_nfa_node(thompson_nfa_node_t* node);
void free_thompson_nfa_description(thompson_nfa_description_t* nfa);

typedef struct {
  alpha_t character; // INVALID_ALPHA for epsilon-transitions
  int destination;   // destination node number
} nfa_transition_t;

typedef struct {
  int num_transitions;
  nfa_transition_t* transitions;
} nfa_node_t;

// Counter of num errors.
typedef uint8_t nfa_errcnt_t;
#define MAX_NFA_ERRCNT 255
typedef uint32_t nfa_errcnt_tmp_t;

// the start state is always 0 
// the accept state is always num_nodes - 1
typedef struct {
  int num_nodes;
  nfa_node_t* nodes;
  int bit_array_len;
  uint64_t* start_states_set; // bit at node_number is whether it's a start state or not
  uint64_t* final_states_set; // bit at node_number is whether it's an end state or not
  regexp_settings_t settings;
} nfa_description_t;

void print_nfa(FILE* f, nfa_description_t* nfa);
void free_nfa_node(nfa_node_t* node);
void free_nfa_description(nfa_description_t* nfa);
error_t init_nfa_description(nfa_description_t* nfa, int num_states);
	
#include "bit_array.h"

#define NFA_MIN(a,b) ( (a)<(b)?(a):(b) )

static inline
void nfa_states_union(int num_nodes, nfa_errcnt_t* dst, nfa_errcnt_t* a, nfa_errcnt_t* b)
{
  for(int i = 0; i < num_nodes; i++ ) {
    dst[i] = NFA_MIN(a[i],b[i]);
  }
}

void nfa_print_states(FILE* f, const char* prefix, const nfa_description_t* nfa, const nfa_errcnt_t* a);

// returns in reachable_characters the bit array representing the
// set of character which are reachable from (marking some transition out of)
// in_states
void approx_get_reachable_characters(const nfa_description_t* nfa,
                              nfa_errcnt_t* in_states,
                              uint64_t* reachable_characters);

int approx_is_final_state(const nfa_description_t* nfa,
                   const nfa_errcnt_t* in_states,
                   nfa_errcnt_tmp_t * num_errors);

void approx_get_start_states(const nfa_description_t* nfa,
                      nfa_errcnt_t* out_states);

void approx_get_reachable_states_allchars(const nfa_description_t* nfa,
                                   nfa_errcnt_tmp_t cost,
                                   const nfa_errcnt_t* in_states,
                                   nfa_errcnt_t* out_states);

// returns in out_states the states reachable from in_states after
// reading the character ch
void approx_get_reachable_states(const nfa_description_t* nfa,
                          alpha_t ch,
                          const nfa_errcnt_t* in_states,
                          nfa_errcnt_t* out_states);

// non-approximate version.
void get_reachable_states(nfa_description_t* nfa,
                          uint64_t* in_states, alpha_t ch,
                          uint64_t* out_states);

void approx_add_error_allchars(const nfa_description_t* nfa,
                        nfa_errcnt_tmp_t cost,
                        const nfa_errcnt_t* in_states,
                        nfa_errcnt_t* out_states);

int upper_triangular_index(int max, int i, int j);
void renumber_all_transitions(nfa_description_t* in, int old, int new_num);
error_t remove_inacessible_states(nfa_description_t* in);
error_t collapse_nfa(nfa_description_t* in);

#endif
