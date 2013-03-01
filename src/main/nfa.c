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

  femto/src/main/nfa.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "buffer.h"
#include "util.h"

#include "nfa.h"

void print_thompson_nfa(FILE* f, thompson_nfa_description_t* nfa)
{
  int ch;
  thompson_nfa_node_t* node;
  fprintf(f, "NFA: \n");
  
  for( int i = 0; i < nfa->num_nodes; i++ ) {
    fprintf(f, "  state %i ", i);
    node = & nfa->nodes[i];
    if( node->character != INVALID_ALPHA ) {
      fprintf(f, "    alpha %x ", node->character);
      ch = node->character - CHARACTER_OFFSET;
      if( isgraph(ch) ) fprintf(f, " '%c' ", ch);
    }
    if( node->character_set ) {
      fprintf(f, "    bit array %p ", node->character_set);
      //fprint_bit_array(f, CHARACTER_SET_CHUNKS, node->character_set,  ALPHA_SIZE_BITS);
      for( int j = 0; j < CHARACTER_SET_CHUNKS; j++ ) {
        fprintf(f, "%" PRIx64 " ", node->character_set[j]);
      }
    }
    if( node->num_epsilon > 0 ) {
      fprintf(f, "    epsilon transitions: ");
      for( int j = 0; j < node->num_epsilon; j++ ) {
        fprintf(f, " -> %i, ", node->epsilon_dst[j]);
      }
    }
    fprintf(f,"\n");
  }
}


void free_thompson_nfa_node(thompson_nfa_node_t* node)
{
  if( node->epsilon_dst ) free( node->epsilon_dst );
  if( node->character_set ) free( node->character_set );
  memset(node, 0, sizeof(thompson_nfa_node_t));
}

void free_thompson_nfa_description(thompson_nfa_description_t* nfa)
{
  for( int i = 0; i < nfa->num_nodes; i++ ) {
    free_thompson_nfa_node(&nfa->nodes[i]);
  }
  free( nfa->nodes );
  memset( nfa, 0, sizeof(thompson_nfa_description_t));
}

void print_nfa(FILE* f, nfa_description_t* nfa)
{
  int ch;
  nfa_node_t* node;
  nfa_transition_t* trans;
  fprintf(f, "NFA: \n");
 
  fprintf(f, " settings cost_bound=%i subst_cost=%i delete_cost=%i insert_cost=%i\n", nfa->settings.cost_bound, nfa->settings.subst_cost, nfa->settings.delete_cost, nfa->settings.insert_cost);

  for( int i = 0; i < nfa->num_nodes; i++ ) {
    fprintf(f, "  state %i ", i);
    node = & nfa->nodes[i];

    if( get_bit( nfa->bit_array_len, nfa->start_states_set, i ) ) {
      fprintf(f, "START ");
    }
    if( get_bit( nfa->bit_array_len, nfa->final_states_set, i ) ) {
      fprintf(f, "FINAL ");
    }
    for( int j = 0; j < node->num_transitions; j++ ) {
      trans = & node->transitions[j];
      if( trans->character != INVALID_ALPHA ) {
        fprintf(f, "    alpha %x ", trans->character);
        ch = trans->character - CHARACTER_OFFSET;
        if( isgraph(ch) ) fprintf(f, " '%c' ", ch);
      } else {
        fprintf(f, "epsilon ");
      }
      fprintf(f, "to %i ", trans->destination);
    }
    fprintf(f,"\n");
  }
}

void free_nfa_node(nfa_node_t* node)
{
  if( node->transitions ) free( node->transitions );
  memset(node, 0, sizeof(nfa_node_t));
}

void free_nfa_description(nfa_description_t* nfa)
{
  for( int i = 0; i < nfa->num_nodes; i++ ) {
    free_nfa_node(&nfa->nodes[i]);
  }
  free( nfa->nodes );
  free( nfa->start_states_set );
  free( nfa->final_states_set );
  memset( nfa, 0, sizeof(nfa_description_t));
}

error_t init_nfa_description(nfa_description_t* nfa, int num_nodes)
{
  memset(nfa, 0, sizeof(nfa_description_t));
  nfa->num_nodes = num_nodes;
  nfa->bit_array_len = CEILDIV(num_nodes, 64);
  nfa->nodes = malloc(nfa->num_nodes * sizeof(nfa_node_t));
  if( ! nfa->nodes ) return ERR_MEM;
  memset( nfa->nodes, 0, nfa->num_nodes * sizeof(nfa_node_t));
  nfa->start_states_set = malloc(nfa->bit_array_len * sizeof(uint64_t));
  if( ! nfa->start_states_set ) return ERR_MEM;
  memset( nfa->start_states_set, 0, nfa->bit_array_len * sizeof(uint64_t));
  nfa->final_states_set = malloc(nfa->bit_array_len * sizeof(uint64_t));
  if( ! nfa->final_states_set ) return ERR_MEM;
  memset( nfa->final_states_set, 0, nfa->bit_array_len * sizeof(uint64_t));

  set_default_regexp_settings(&nfa->settings);
  return ERR_NOERR;
}

void nfa_print_states(FILE* f, const char* prefix, const nfa_description_t* nfa, const nfa_errcnt_t* a)
{
  for(int i = 0; i < nfa->num_nodes; i++ ) {
    if( a[i] < nfa->settings.cost_bound ) {
      fprintf(f, "%s", prefix);
      if( get_bit(nfa->bit_array_len, nfa->start_states_set, i) )
        fprintf(f, "initial ");
      if( get_bit(nfa->bit_array_len, nfa->final_states_set, i) )
        fprintf(f, "final ");
      fprintf(f, "state %i at cost %i\n", i, (int) a[i]);
    }
  }
}

void approx_get_reachable_characters(const nfa_description_t* nfa,
                              nfa_errcnt_t* in_states,
                              uint64_t* reachable_characters)
{
  // clear reachable_characters
  memset(reachable_characters, 0, CHARACTER_SET_CHUNKS*sizeof(uint64_t));

  if( EXTRA_CHECKS ) {
    assert( nfa->settings.cost_bound >= 1 );
    assert( nfa->settings.subst_cost >= 1 );
    assert( nfa->settings.delete_cost >= 1 );
    assert( nfa->settings.insert_cost >= 1 );
  }

  // for each of the in states
  for (int i = 0; i < nfa->num_nodes; i++) {
    nfa_errcnt_tmp_t err_to_here = in_states[i];

    // is it reachable?
    if( err_to_here < nfa->settings.cost_bound ) {
      int num_trans = nfa->nodes[i].num_transitions;

      for (int j = 0; j < num_trans; j++) {
        int character;

	// if it is a character transition
	if( EXTRA_CHECKS ) assert(nfa->nodes[i].transitions[j].character != INVALID_ALPHA);

        // store the destination
        character = nfa->nodes[i].transitions[j].character;

        // set the corresponding bit in out_states
        set_bit (CHARACTER_SET_CHUNKS, reachable_characters, character);
	// else it's not a character transition, do nothing
      }
    }
    // else the bit is not set, do nothing
  }
}

int approx_is_final_state(const nfa_description_t* nfa,
                   const nfa_errcnt_t* in_states,
                   nfa_errcnt_tmp_t * num_errors)
{
  for(int i = 0; i < nfa->num_nodes; i++ ) {
    if( in_states[i] < nfa->settings.cost_bound &&
        get_bit( nfa->bit_array_len, nfa->final_states_set, i ) ) {
      *num_errors = in_states[i];
      return 1;
    }
  }
  return 0;
}


void approx_get_start_states(const nfa_description_t* nfa,
                      nfa_errcnt_t* out_states)
{
  // clear out_states
  for(int i = 0; i < nfa->num_nodes; i++ ) {
    
    if( get_bit( nfa->bit_array_len, nfa->start_states_set, i ) ) {
      out_states[i] = 0; // start state!
    } else {
      out_states[i] = MAX_NFA_ERRCNT;
    }
  }
}

void approx_get_reachable_states_allchars(const nfa_description_t* nfa,
                                   nfa_errcnt_tmp_t cost,
                                   const nfa_errcnt_t* in_states,
                                   nfa_errcnt_t* out_states)
{
  // clear out_states
  for(int i = 0; i < nfa->num_nodes; i++ ) {
    out_states[i] = MAX_NFA_ERRCNT;
  }

  // Can't do any better if we can't have any errors.
  if( nfa->settings.cost_bound <= 1 ) return;

  // for each of the in_states
  for (int i = 0; i < nfa->num_nodes; i++) {
    nfa_errcnt_tmp_t err_to_here = in_states[i];

    // read all characters and add one to the error count.

    // is it reachable?
    if( err_to_here + cost < nfa->settings.cost_bound ) {
      int num_trans = nfa->nodes[i].num_transitions;

      for (int j = 0; j < num_trans; j++) {
        int ch = nfa->nodes[i].transitions[j].character;
        // take the transition and say we 'deleted'
        
	if (CHARACTER_OFFSET <= ch && ch < ALPHA_SIZE ) {
          // store the destination
          int node = nfa->nodes[i].transitions[j].destination;

          out_states[node] = NFA_MIN(out_states[node], err_to_here + cost);
        }
      }
    }
    // else the state is not reachable, do nothing
  }
}

void approx_get_reachable_states(const nfa_description_t* nfa,
                                 alpha_t ch,
                                 const nfa_errcnt_t* in_states,
                                 nfa_errcnt_t* out_states)
{
  // clear out_states
  for(int i = 0; i < nfa->num_nodes; i++ ) {
    out_states[i] = MAX_NFA_ERRCNT;
  }

  // for each of the in_states
  for (int i = 0; i < nfa->num_nodes; i++) {
    nfa_errcnt_tmp_t err_to_here = in_states[i];

    // is it reachable?
    if( err_to_here < nfa->settings.cost_bound ) {
      int num_trans = nfa->nodes[i].num_transitions;

      for (int j = 0; j < num_trans; j++) {
        // if the character matches what we're looking for
	if (nfa->nodes[i].transitions[j].character == ch) {
	  // store the destination
	  int node = nfa->nodes[i].transitions[j].destination;

	  // set the corresponding bit in out_states
          out_states[node] = NFA_MIN(out_states[node], err_to_here);
	} // else the character does not match, do nothing
      }
    } // else the state is not reachable, do nothing
  }
}

void approx_add_error_allchars(const nfa_description_t* nfa,
                               nfa_errcnt_tmp_t cost,
                               const nfa_errcnt_t* in_states,
                               nfa_errcnt_t* out_states)
{
  // clear out_states
  for(int i = 0; i < nfa->num_nodes; i++ ) {
    out_states[i] = MAX_NFA_ERRCNT;
  }

  // for each of the in_states
  for (int i = 0; i < nfa->num_nodes; i++) {
    nfa_errcnt_tmp_t err_to_here = in_states[i];

    // add the error count.
    out_states[i] = NFA_MIN(out_states[i], err_to_here + cost);
  }
}

void get_reachable_states(nfa_description_t* nfa,
                          uint64_t* in_states, alpha_t ch,
                          uint64_t* out_states)
{
  // clear out_states
  memset(out_states, 0, nfa->bit_array_len*sizeof(uint64_t));

  // for each of the in_states
  for (int i = 0; i < nfa->num_nodes; i++) {
    // if the bit is set
    if ( get_bit(nfa->bit_array_len, in_states, i) == 1) {
      int num_trans = nfa->nodes[i].num_transitions;

      for (int j = 0; j < num_trans; j++) {
	// if the character matches what we're looking for
	if (nfa->nodes[i].transitions[j].character == ch) {
	  // store the destination
	  int node = nfa->nodes[i].transitions[j].destination;

	  // set the corresponding bit in out_states
	  set_bit (nfa->bit_array_len, out_states, node);
	}
	// else the character does not match, do nothing
      }
    }
    // else the bit is not set, do nothing
  }
}

void get_reachable_characters(nfa_description_t* nfa,
                              uint64_t* in_states,
                              uint64_t* reachable_characters)
{
  // clear reachable_characters
  memset(reachable_characters, 0, CHARACTER_SET_CHUNKS*sizeof(uint64_t));

  // for each of the in states
  for (int i = 0; i < nfa->num_nodes; i++) {
    // if the bit is set
    if ( get_bit(nfa->bit_array_len, in_states, i) == 1) {
      int num_trans = nfa->nodes[i].num_transitions;

      for (int j = 0; j < num_trans; j++) {
	// if it is a character transition
	if (nfa->nodes[i].transitions[j].character != INVALID_ALPHA) {
	  // store the destination
	  int character = nfa->nodes[i].transitions[j].character;

	  // set the corresponding bit in out_states
	  set_bit (CHARACTER_SET_CHUNKS, reachable_characters, character);
	}
	// else it's not a character transition, do nothing
      }
    }
    // else the bit is not set, do nothing
  }
}

void renumber_all_transitions(nfa_description_t* in, int old, int new_num)
{
  // wherever old occurs in transitions, replace it with new.
  for( int i = 0; i < in->num_nodes; i++ ) {
    for( int t = 0; t < in->nodes[i].num_transitions; t++ ) {
      if( in->nodes[i].transitions[t].destination == old ) {
        in->nodes[i].transitions[t].destination = new_num;
      }
    }
  }
}

int sort_transitions_by_char_then_dest(const void* aP, const void* bP)
{
  nfa_transition_t* a = (nfa_transition_t*) aP;
  nfa_transition_t* b = (nfa_transition_t*) bP;

  if( a->character < b->character ) return -1;
  if( a->character > b->character ) return 1;
  if( a->destination < b->destination ) return -1;
  if( a->destination > b->destination ) return 1;
  return 0;
}

int sort_transitions_by_dest_then_char(const void* aP, const void* bP)
{
  nfa_transition_t* a = (nfa_transition_t*) aP;
  nfa_transition_t* b = (nfa_transition_t*) bP;

  if( a->destination < b->destination ) return -1;
  if( a->destination > b->destination ) return 1;
  if( a->character < b->character ) return -1;
  if( a->character > b->character ) return 1;
  return 0;
}

error_t remove_inacessible_states(nfa_description_t* in)
{
  uint64_t* states;
  uint64_t* reachable_states;
  int changed;

  states = allocate_bit_array(in->bit_array_len);
  if( ! states ) return ERR_MEM;
  reachable_states = allocate_bit_array(in->bit_array_len);
  if( ! reachable_states ) return ERR_MEM;

  memcpy(states, in->start_states_set, in->bit_array_len * sizeof(uint64_t));
  // now run the NFA until we don't have any more bits to set.
  do {
    changed = 0;
    for( alpha_t ch = 0; ch < ALPHA_SIZE; ch++ ) {
      get_reachable_states(in, states, ch, reachable_states);
      // combine reachable states with states.
      bit_array_or(in->bit_array_len, reachable_states, states, reachable_states);
      // check to see if anything changed.
      if( 0 != bit_array_cmp(in->bit_array_len, states, reachable_states) ) {
        changed++;
        memcpy(states, reachable_states, in->bit_array_len * sizeof(uint64_t));
      }
    }
  } while (changed);

  // now, remove states that aren't represented, and renumber and
  // relocate when there are gaps.
  {
    int *mapping = malloc(in->num_nodes*sizeof(int));
    int idx = 0;

    if( ! mapping ) return ERR_MEM;

    // goes to -1 for removing, or to an integer for replacing.
    for( int i = 0; i < in->num_nodes; i++ ) {
      if( get_bit(in->bit_array_len, states, i) ) {
        // a keeper!
        mapping[i] = idx++;
      } else {
        // lose the extra!
        mapping[i] = -1;
      }
    }

    for( int i = 0; i < in->num_nodes; i++ ) {
      if( mapping[i] < 0 ) {
        // node i is inacessible - free it.
        free_nfa_node(&in->nodes[i]);
        renumber_all_transitions(in, i, in->num_nodes);
      }
      if( mapping[i] >= 0 && mapping[i] != i ) {
        // copy the i'th entry to mapping[i].
        memcpy(&in->nodes[mapping[i]], &in->nodes[i], sizeof(nfa_node_t));
        // renumber any transitions in the entire NFA
        renumber_all_transitions(in, i, mapping[i]);
        // move over the value from start_states and final_states sets.
        assign_bit(in->bit_array_len, in->start_states_set, mapping[i],
                   get_bit(in->bit_array_len, in->start_states_set, i)); 
        assign_bit(in->bit_array_len, in->final_states_set, mapping[i],
                   get_bit(in->bit_array_len, in->final_states_set, i)); 
      }
    }

    in->num_nodes = idx;
    // reallocate the nodes
    {
      nfa_node_t* new_nodes;
      new_nodes = realloc(in->nodes, in->num_nodes * sizeof(nfa_node_t));
      if( ! new_nodes ) return ERR_MEM;
      in->nodes = new_nodes;
    }

    free(mapping);

    // sort the transitions and collapse any duplicates.
    for( int i = 0; i < in->num_nodes; i++ ) {
      nfa_node_t* node = &in->nodes[i];
      // sort first by destination, then by character.
      int num;
      num = sort_dedup(node->transitions,
                       node->num_transitions, 
                       sizeof(nfa_transition_t),
                       sort_transitions_by_dest_then_char);
      // decrease num until we're at transitions that don't leave the NFA.
      for( int j = num - 1; j >= 0; j-- ) {
        if( node->transitions[j].destination >= in->num_nodes ) {
          num--;
        } else {
          break;
        }
      }
      node->num_transitions = num;

      // reallocate the transitions.
      {
        nfa_transition_t* new_t;
        new_t = realloc(node->transitions, num*sizeof(nfa_transition_t));
        if( num > 0 && ! new_t ) return ERR_MEM;
        node->transitions = new_t;
      }
      
      // sort the transitions by character then destination.
      qsort(node->transitions, node->num_transitions,
            sizeof(nfa_transition_t),
            sort_transitions_by_char_then_dest);

    }
  }

  free_bit_array(reachable_states);
  free_bit_array(states);

  return ERR_NOERR;
}



// upper-triangular index
int upper_triangular_index(int max, int i, int j)
{
  int temp;
  // swap if i > j
  if( i > j ) {
    temp = i;
    i = j;
    j = temp;
  }
  // anchored at 0 in the upper-right
  int a = max - j + i;
  return a*(a+1)/2 + i;
  // 0 in the lower-left?
  //int a = max - 1 - i;
  //return a*(a+1)/2 + max - j;
}
static inline
int uti(int max, int i, int j)
{
  return upper_triangular_index(max, i, j);
}

#define COLLAPSE_DEBUG 0

static int step3(nfa_description_t* in, int len, uint64_t* table,
                  int i, int j) 
{
  int max = in->num_nodes - 1;
  int changed = 0;
  for( int t = 0; t < in->nodes[i].num_transitions; t++ ) {
    alpha_t a = in->nodes[i].transitions[t].character;
    int i_prime = in->nodes[i].transitions[t].destination;
    int check = 1;
    // we have an i_prime in destinations from i with a
    // check that for all j_prime in destinations from j with a
    // that (i_prime, j_prime) is marked.

    for( int s = 0; s < in->nodes[j].num_transitions; s++ ) {
      if( in->nodes[j].transitions[s].character == a ) {
        int j_prime = in->nodes[j].transitions[s].destination;
        if( i == j || // i==j is never marked.
            ! get_bit(len, table, uti(max, i_prime, j_prime)) ) {
          // one isn't marked -- the for all is false
          check = 0;
          break;
        }
      }
    }

    // all satisfactory (i_prime, j_prime) were marked
    if( check ) {
      // mark (i, j)
      if( COLLAPSE_DEBUG ) {
        printf("marking (%i, %i)\n", i, j);
      }
      set_bit(len, table, uti(max, i, j));
      changed++;
    }
  }
  return changed;
}


error_t collapse_nfa(nfa_description_t* in)
{
  error_t err;
  // This NFA collapsing follows the algorithm outlined in 
  // p 106 of the chapter "Collapsing Nondetermininstic Automata"
  // in Automata and Computability by Dexter C. Kozen.

  // 1. Write down a table of all pairs (p,q) initially unmarked.
  // This table will be represented with a bit-array. 
  // The diagonal is not stored. 
  // The (i,j) 

  int max = in->num_nodes - 1;
  int len = 1 + uti(max, max - 1, max);
  uint64_t* table;
  int changed;
  len = CEILDIV(len, 64);

  table = allocate_bit_array(len);
  if( ! table ) return ERR_MEM;
  // the table of pairs is initially unmarked, because allocate_bit-array zeros.

  if( COLLAPSE_DEBUG ) {
    printf("Input NFA\n");
    print_nfa(stdout, in);
  }

  // remove inacessible states.
  err = remove_inacessible_states(in);
  if( err ) return err;

  if( COLLAPSE_DEBUG ) {
    printf("After initially removing inacessible states\n");
    print_nfa(stdout, in);
  }

  // 2. Mark (p,q) if p in final states and q not in final states.
  for( int j = 1; j < in->num_nodes; j++ ) {
    for( int i = 0; i < j; i++ ) {
      int ifinal = get_bit(in->bit_array_len, in->final_states_set, i);
      int jfinal = get_bit(in->bit_array_len, in->final_states_set, j);
      if( ifinal != jfinal ) {
        if( COLLAPSE_DEBUG ) {
          printf("initially marking (%i, %i)\n", i, j);
        }
        set_bit(len, table, uti(max, i, j));
      }
    }
  }

  // 3. Repeat until no more changes occur.
  do {
    changed = 0;
    // if (p,q) is unmarked, and if for some a in the alphabet, 
    // there exists p' a destination of p with a such that for
    // all q' a destination of q with a, (p',q') is marked -- 
    // then mark (p,q).
    for( int j = 1; j < in->num_nodes; j++ ) {
      for( int i = 0; i < j; i++ ) {
        if( 0 == get_bit(len, table, uti(max, i,j))) {
          // (i,j) is unmarked.
          // find i' a destination of i with a
          changed += step3(in, len, table, i, j);
          changed += step3(in, len, table, j, i);
        }
      }
    }

  } while( changed > 0 );

  // finally, define p == q iff (p,q) is never marked.
  for( int j = 1; j < in->num_nodes; j++ ) {
    for( int i = 0; i < j; i++ ) {
      if( ! get_bit(len, table, uti(max, i, j)) ) {
        // i,j was never marked!
        // i and j are the same node
        // replace all j with i (i < j, so this will keep the
        // node numbers low).
        renumber_all_transitions(in, j, i);
        // remove j from the start states.
        clear_bit(in->bit_array_len, in->start_states_set, j);
      }
    }
  }
  free_bit_array(table);

  // remove inacessible states.
  err = remove_inacessible_states(in);
  if( err ) return err;

  if( COLLAPSE_DEBUG ) {
    printf("After finally removing inacessible states\n");
    print_nfa(stdout, in);
  }

  return ERR_NOERR;
}
#undef COLLAPSE_DEBUG



