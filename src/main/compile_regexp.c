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

  femto/src/main/compile_regexp.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "posix.bison.h"
#include "ast.h"
#include "nfa.h"
#include "error.h"
#include "bit_funcs.h"
#include <assert.h>

#include "util.h"


error_t append_node(thompson_nfa_description_t* dst, thompson_nfa_node_t* node)
{
  return append_array(&dst->num_nodes, & dst->nodes, sizeof(thompson_nfa_node_t), node);
}

/* Appends one NFA to another. Does not connect them with transitions;
   just appends the nodes to the dst. array and renumbers them.
   If overlap is set, and dst has any nodes, the final node in dst
   will be replaced with the first node in nfa.
   */
error_t append_nfa(thompson_nfa_description_t* dst, thompson_nfa_description_t* nfa, int overlap)
{
  int idx;
  int offset;
  thompson_nfa_node_t* node;
  error_t err;

  if( ! (overlap == 0 || overlap == 1 ) ) return ERR_PARAM;

  // only overlap if there's more than one dst node.
  if( dst->num_nodes == 0 ) overlap = 0;

  // subtract one if we're going to merge final and initial.
  offset = dst->num_nodes - overlap;

  for( int i = 0; i < nfa->num_nodes; i++ ) {
    if( i == 0 && overlap ) {
      idx = dst->num_nodes - 1;
      free_thompson_nfa_node(&dst->nodes[idx]);
      memcpy(&dst->nodes[idx], &nfa->nodes[i], sizeof(thompson_nfa_node_t));
    } else {
      idx = dst->num_nodes;
      err = append_node(dst, &nfa->nodes[i]);
      if( err ) return err;
    }

    node = &dst->nodes[idx];
    // reallocate the character set.
    if( node->character_set ) {
      uint64_t* old = node->character_set;
      node->character_set = malloc(sizeof(uint64_t)*SET_NODE_CHUNKS);
      if( ! node->character_set ) return ERR_MEM;

      memcpy( node->character_set, old, sizeof(uint64_t)*SET_NODE_CHUNKS);
    }
    // reallocate the epsilon transitions.
    if( node->num_epsilon > 0 ) {
      int* old = node->epsilon_dst;
      node->epsilon_dst = malloc(sizeof(int)*node->num_epsilon);
      if( ! node->epsilon_dst ) return ERR_MEM;

      // now update dst->nodes[idx] to fix the pointers.
      for( int j = 0; j < node->num_epsilon; j++ ) {
        node->epsilon_dst[j] = old[j] + offset;
      }
    }
  }

  return ERR_NOERR;

}


error_t add_character_node(thompson_nfa_description_t* dst, alpha_t character)
{
  thompson_nfa_node_t node;
  node.character = character;
  node.character_set = NULL;
  node.num_epsilon = 0;
  node.epsilon_dst = NULL;
  return append_node(dst, &node);
}

error_t add_epsilon_node(thompson_nfa_description_t* dst)
{
  thompson_nfa_node_t node;
  node.character = INVALID_ALPHA;
  node.character_set = NULL;
  node.num_epsilon = 0;
  node.epsilon_dst = NULL;
  return append_node(dst, &node);
}

/* Adds an epsilon transition from node_num to dst.
 */
error_t add_epsilon_transition(thompson_nfa_description_t* nfa, int node_num, int dst)
{
  thompson_nfa_node_t* node;

  if( node_num >= nfa->num_nodes ) return ERR_PARAM;

  node = &nfa->nodes[node_num];

  return append_array(&node->num_epsilon, &node->epsilon_dst, sizeof(int), &dst);
}

error_t compile_regexp_thompson(thompson_nfa_description_t* nfa, struct ast_node* ast)
{
  error_t err;
  thompson_nfa_description_t* machs = NULL;
  int nmachs = 0;
  int sub_nodes, first, last, initial, final;

  // Does Thompson's construction.
  memset(nfa, 0, sizeof(thompson_nfa_description_t));

  switch (ast->type) {
    case AST_NODE_REGEXP:
      {
        struct regexp_node* n = (struct regexp_node*) ast;

        nmachs = n->choices.num;
        // we'll store the machines we're putting together in machs.
        machs = malloc(sizeof(thompson_nfa_description_t)*nmachs);
        if( ! machs ) { err = ERR_MEM; goto error; }

        // make the recursive call to create the machines
        sub_nodes = 0;
        for( int i = 0; i < nmachs; i++ ) {
          err = compile_regexp_thompson(&machs[i], n->choices.list[i]);
          if( err ) goto error;
          sub_nodes += machs[i].num_nodes;
        }

        if( nmachs == 1 ) {
          // only one machine - just copy that machine!
          err = append_nfa(nfa, &machs[0], 0);
          if( err ) goto error;
        } else {
          // add a start node.
          err = add_epsilon_node(nfa);
          if( err ) goto error;

          initial = 0;
          final = sub_nodes + 1; // 1 for initial state.

          // add the sub-machines.
          for( int i = 0; i < nmachs; i++ ) {
            first = nfa->num_nodes;
            err = append_nfa(nfa, &machs[i], 0);
            if( err ) goto error;
            last = nfa->num_nodes - 1;
            // add the epsilon transition from the initial to first
            err = add_epsilon_transition(nfa, 0, first);
            if( err ) goto error;
            // add the epsilon transition from last to final
            err = add_epsilon_transition(nfa, last, final);
            if( err ) goto error;
          }
          // add the final node.
          err = add_epsilon_node(nfa);
          if( err ) goto error;
          assert(nfa->num_nodes - 1 == final);
        }

      }
      break;
    case AST_NODE_SEQUENCE:
      {
        struct sequence_node* n = (struct sequence_node*) ast;

        // we'll store the machines we're putting together in machs.
        nmachs = n->atoms.num;
        machs = malloc(sizeof(thompson_nfa_description_t)*nmachs);
        if( ! machs ) { err = ERR_MEM; goto error; }

        // make the recursive call to create the machines
        sub_nodes = 0;
        for( int i = 0; i < nmachs; i++ ) {
          err = compile_regexp_thompson(&machs[i], n->atoms.list[i]);
          if( err ) return err;
          sub_nodes += machs[i].num_nodes;
        }

        // add the machines
        for( int i = 0; i < nmachs; i++ ) {
          err = append_nfa(nfa, &machs[i], 1);
          if( err ) goto error;
        }

      }
      break;
    case AST_NODE_ATOM:
      {
        struct atom_node* n = (struct atom_node*) ast;
        // an atom has just some number of repeats...
        nmachs = 1;
        machs = malloc(sizeof(thompson_nfa_description_t)*nmachs);
        if( ! machs ) { err = ERR_MEM; goto error; }

        err = compile_regexp_thompson(&machs[0], n->child);
        if( err ) goto error;
        sub_nodes = machs[0].num_nodes;

        // now add the required number.
        for( int i = 0; i < n->repeat.min; i++ ) {
          // with overlap..
          err = append_nfa(nfa, &machs[0], 1);
          if( err ) goto error;
        }
        if( n->repeat.min == 0 ) {
          err = add_epsilon_node(nfa);
          if( err ) goto error;
        }

        // now add the optional number, or the optional unbounded case.
        if( n->repeat.max == UNBOUNDED_REPEATS ) {
          // add the optional unbounded case - *
          initial = nfa->num_nodes - 1;
          final = initial + 1 + sub_nodes;

          first = nfa->num_nodes;
          err = append_nfa(nfa, &machs[0], 0);
          if( err ) goto error;
          last = nfa->num_nodes - 1;

          // add the final node
          err = add_epsilon_node(nfa);
          if( err ) goto error;

          // add the transitions:
          // initial->first
          // last->first
          // last->final
          // initial->final
          err = add_epsilon_transition(nfa, initial, first);
          if( err ) goto error;
          err = add_epsilon_transition(nfa, last, first);
          if( err ) goto error;
          err = add_epsilon_transition(nfa, last, final);
          if( err ) goto error;
          err = add_epsilon_transition(nfa, initial, final);
          if( err ) goto error;

        } else {
          // add the optional cases up to max.
          int num = n->repeat.max - n->repeat.min;

          for( int i = 0; i < num; i++ ) {
            // add an initial node.
            initial = nfa->num_nodes - 1;

            first = nfa->num_nodes;
            err = append_nfa(nfa, &machs[0], 0);
            if( err ) goto error;
            final = nfa->num_nodes - 1;

            // add a transition from initial to first
            err = add_epsilon_transition(nfa, initial, first);
            if( err ) goto error;

            // add a transition from initial to final
            err = add_epsilon_transition(nfa, initial, final);
            if( err ) goto error;
          }

        }
      }

      break;
    case AST_NODE_SET:
      {
        struct set_node* n = (struct set_node*) ast;
        uint64_t* set = malloc(SET_NODE_CHUNKS*sizeof(uint64_t));
        if( ! set ) return ERR_MEM;

        memcpy(set, n->bits, SET_NODE_CHUNKS*sizeof(uint64_t));

        // add the initial node
        err = add_character_node(nfa, INVALID_ALPHA);
        if( err ) goto error;

        // set the character set for that node.
        nfa->nodes[nfa->num_nodes - 1].character_set = set;

        // add the final node
        err = add_epsilon_node(nfa);
        if( err ) goto error;
      }
      break;
    case AST_NODE_CHARACTER:
      {
        struct character_node* n = (struct character_node*) ast;

        // add the initial node
        err = add_character_node(nfa, n->ch);
        if( err ) goto error;

        // add the final node
        err = add_epsilon_node(nfa);
        if( err ) goto error;
      }
      break;
    case AST_NODE_STRING:
      {
        struct string_node* n = (struct string_node*) ast;

        // construct a sequence...
        for( int i = 0; i < n->string.len; i++ ) {
          err = add_character_node(nfa, n->string.chars[i]);
          if( err ) goto error;
        }

        // add the final node
        err = add_epsilon_node(nfa);
        if( err ) goto error;
 
      }
      break;
    case AST_NODE_RANGE:
      return ERR_PARAM;
      break;
    default:
      return ERR_PARAM;
  }

  if(nfa->num_nodes <= 0 ) {
    return ERR_PARAM;
  }

  //print_nfa(stdout, nfa);
  err = ERR_NOERR;

error:
  if( machs ) {
    for(int i = 0; i < nmachs; i++ ) {
      free_thompson_nfa_description(&machs[i]);
    }
    free(machs);
  }
  if( err ) {
    free_thompson_nfa_description(nfa);
  }
  return err;

}

error_t compile_regexp_from_string_thompson(thompson_nfa_description_t* nfa, char* regexp)
{
  error_t err;
  struct ast_node* root;

  root = parse_string(strlen(regexp), regexp);
  if( ! root ) return ERR_PARAM;

  err = compile_regexp_thompson(nfa, root);

  // free the ast.
  free_ast_node(root);

  return err;
}

error_t convert_thompson_nfa(nfa_description_t* nfa, thompson_nfa_description_t* tnfa)
{
  if( DEBUG > 2 ) {
   fprintf(stdout, "Converting Thompson NFA:\n");
   print_thompson_nfa(stdout, tnfa);
  }

  // create as many nodes for the other nfa.
  memset(nfa, 0, sizeof(nfa_description_t));
  nfa->num_nodes = tnfa->num_nodes;
  nfa->nodes = malloc(tnfa->num_nodes*sizeof(nfa_node_t));
  if( ! nfa->nodes ) return ERR_MEM;
  nfa->bit_array_len = CEILDIV(tnfa->num_nodes, 64);
  nfa->start_states_set = allocate_bit_array(nfa->bit_array_len);
  if( ! nfa->start_states_set ) return ERR_MEM;
  nfa->final_states_set = allocate_bit_array(nfa->bit_array_len);
  if( ! nfa->final_states_set ) return ERR_MEM;

  // set the start and end states.
  set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
  set_bit(nfa->bit_array_len, nfa->final_states_set, tnfa->num_nodes - 1);

  // convert from a thompson NFA to a regular one.
  for( int i = 0; i < tnfa->num_nodes; i++ ) {
    thompson_nfa_node_t* tn = & tnfa->nodes[i];
    nfa_node_t* n = & nfa->nodes[i];
    n->num_transitions = tn->num_epsilon;
    if( tn->character != INVALID_ALPHA ) {
      n->num_transitions++;
    }
    if( tn->character_set ) {
      for( int j = 0; j < ALPHA_SIZE; j++ ) {
        if( get_bit( CHARACTER_SET_CHUNKS, tn->character_set, j) ) {
          n->num_transitions++;
        }
      }
    }
    // allocate room for the transitions
    n->transitions = NULL;
    if( n->num_transitions > 0 ) {
      int k = 0; // output transition number

      n->transitions = malloc(n->num_transitions*sizeof(nfa_transition_t));
      if( ! n->transitions ) return ERR_MEM;

      // set the transitions.
      // add the epsilon transitions
      for( int j = 0; j < tn->num_epsilon; j++ ) {
        n->transitions[k].character = INVALID_ALPHA;
        n->transitions[k].destination = tn->epsilon_dst[j];
        k++;
      }
      // add the character
      if( tn->character != INVALID_ALPHA ) {
        n->transitions[k].character = tn->character;
        n->transitions[k].destination = i+1; // the next node
        k++;
      }
      // add the character set ones.
      if( tn->character_set ) {
        for( int j = 0; j < ALPHA_SIZE; j++ ) {
          if( get_bit( CHARACTER_SET_CHUNKS, tn->character_set, j) ) {
            n->transitions[k].character = j;
            n->transitions[k].destination = i+1; // the next node
            k++;
          }
        }
      }
    }
  }

  if( DEBUG > 2 ) {
    fprintf(stdout, "To NFA:\n");
    print_nfa(stdout, nfa);
  }

  return ERR_NOERR;
}

error_t remove_unreachable(nfa_description_t* in, nfa_description_t* out)
{
  uint64_t* states;
  int changed;
  int* new_numbers;
  nfa_node_t* node;
  nfa_transition_t* trans;
  error_t err;
  
  states = malloc(in->bit_array_len*sizeof(uint64_t));

  memcpy(states, in->start_states_set, in->bit_array_len*sizeof(uint64_t));
  // compute the states reachable from the start states
  changed = 1;
  while(changed > 0 ) {
    changed = 0;
    for( int j = 0; j < in->num_nodes; j++ ) {
      if( get_bit( in->bit_array_len, states, j ) ) {
        // for each node we're at
        node = & in->nodes[j];
        for( int k = 0; k < node->num_transitions; k++ ) {
          trans = & node->transitions[k];
          if( ! get_bit( in->bit_array_len, states, trans->destination ) ) {
            // only increment changed if it's not already set
            set_bit(in->bit_array_len, states, trans->destination);
            changed++;
          }
        }
      }
    }
  }

  // now states is the set of reachable nodes.
  new_numbers = malloc(in->num_nodes*sizeof(int));
  changed = 0;

  // compute the new numbers... mapping from old to new.
  for( int i = 0; i < in->num_nodes; i++ ) {
    if( get_bit(in->bit_array_len, states, i ) ) {
      // if the bit is set 
      new_numbers[i] = changed;
      changed++; 
    } else {
      new_numbers[i] = -1; // to remove!
    }
  }

  // now remove the states we don't like and compute the new ones!
  err = init_nfa_description(out, changed);
  if( err ) return err;

  // copy over each node we're supposed to keep.
  changed = 0;
  for( int i = 0; i < in->num_nodes; i++ ) {
    if( get_bit( in->bit_array_len, in->start_states_set, i ) ) {
      assert( new_numbers[i] >= 0 );
      set_bit( out->bit_array_len, out->start_states_set, new_numbers[i] );
    }
    if( get_bit(in->bit_array_len, states, i ) ) {
      if( get_bit( in->bit_array_len, in->final_states_set, i ) ) {
        assert( new_numbers[i] >= 0 );
        set_bit( out->bit_array_len, out->final_states_set, new_numbers[i] );
      }
      node = & in->nodes[i];
      out->nodes[changed].num_transitions = node->num_transitions;
      out->nodes[changed].transitions = malloc(node->num_transitions*sizeof(nfa_transition_t));
      if( ! out->nodes[changed].transitions ) return ERR_MEM;
      for( int k = 0; k < node->num_transitions; k++ ) {
        trans = &node->transitions[k];
        assert(new_numbers[trans->destination] >= 0 );
        out->nodes[changed].transitions[k].character = trans->character;
        out->nodes[changed].transitions[k].destination = new_numbers[trans->destination];
      }
      changed++;
    }
  }

  free(new_numbers);
  free(states);

  return ERR_NOERR;
}

error_t remove_epsilon(nfa_description_t* in, nfa_description_t* out_desc)
{
  error_t err;
  nfa_description_t out_space;
  nfa_description_t* out = &out_space;
  uint64_t* states;
  nfa_node_t* node;
  nfa_transition_t* trans;
  int changed;
  int new_trans;


  err = init_nfa_description(out, in->num_nodes);
  if( err ) return err;
  
  states = malloc(out->bit_array_len*sizeof(uint64_t));
  if( ! states ) return ERR_MEM;

  for (int i = 0; i < in->num_nodes; i++ ) {
    // compute the set of reachable states by epsilon transitions.
    memset(states, 0, in->bit_array_len*sizeof(uint64_t));
    // set the bit at i
    set_bit(in->bit_array_len, states, i);
    changed = 1;
    while( changed > 0 ) {
      changed = 0;
      for( int j = 0; j < in->num_nodes; j++ ) {
        if( get_bit( in->bit_array_len, states, j ) ) {
          // for each node we're at
          node = & in->nodes[j];
          for( int k = 0; k < node->num_transitions; k++ ) {
            trans = & node->transitions[k];
            if( trans->character == INVALID_ALPHA ) {
              if( ! get_bit( in->bit_array_len, states, trans->destination ) ) {
                // only increment changed if it's not already set
                set_bit(in->bit_array_len, states, trans->destination);
                changed++;
              }
            }
          }
        }
      }
    }
    // at this point, we've computed the nodes reachable
    // by epsilon transitions from node i.

    if( i == 0 ) {
      // 0 is the start state... set the start states.
      //memcpy(out->start_states_set, states, out->bit_array_len*sizeof(uint64_t));
      set_bit(out->bit_array_len, out->start_states_set, i);
    }
    if( get_bit(out->bit_array_len, states, in->num_nodes - 1 ) ) {
      // the final state is reachable from this node.
      // this node will be a final state.
      set_bit(out->bit_array_len, out->final_states_set, i);
    }

    // create a transition for each of these nodes and for each
    // of the epsilon transitions.
    new_trans = 0;
    // count any character transitions.
    for( int j = 0; j < in->num_nodes; j++ ) {
      if( get_bit(in->bit_array_len, states, j ) ) {
        // for each state reachable from i by epsilon transitions.
        node = & in->nodes[j];
        for( int k = 0; k < node->num_transitions; k++ ) {
          // for each transition
          trans = & node->transitions[k];
          if( trans->character != INVALID_ALPHA ) {
            new_trans++;
          }
        }
      }
    }
    // add the transitions.
    out->nodes[i].num_transitions = new_trans;
    out->nodes[i].transitions = malloc(new_trans * sizeof(nfa_transition_t));
    if( !  out->nodes[i].transitions ) return ERR_MEM;

    new_trans = 0;
    // add any character transitions.
    for( int j = 0; j < in->num_nodes; j++ ) {
      if( get_bit(in->bit_array_len, states, j ) ) {
        // for each state reachable from i by epsilon transitions.
        node = & in->nodes[j];
        for( int k = 0; k < node->num_transitions; k++ ) {
          // for each transition
          trans = & node->transitions[k];
          if( trans->character != INVALID_ALPHA ) {
            out->nodes[i].transitions[new_trans].character = trans->character;
            out->nodes[i].transitions[new_trans].destination = trans->destination;
            new_trans++;
          }
        }
      }
    }
  }

  free(states);

  err = remove_unreachable(out, out_desc);
  if( err ) return err;
  free_nfa_description(out);

  // all done!
  return ERR_NOERR;
}

error_t compile_regexp_from_ast(nfa_description_t* nfa, struct ast_node* ast)
{
  error_t err;
  thompson_nfa_description_t tnfa;
  nfa_description_t with_eps;
  regexp_settings_t s;

  set_default_regexp_settings(&s);

  // Copy settings.
  switch (ast->type) {
    case AST_NODE_REGEXP:
      {
        struct regexp_node* n = (struct regexp_node*) ast;

        s.cost_bound = n->s.cost_bound;
        s.subst_cost = NFA_MIN(n->s.subst_cost, MAX_NFA_ERRCNT);
        s.delete_cost = NFA_MIN(n->s.delete_cost, MAX_NFA_ERRCNT);
        s.insert_cost = NFA_MIN(n->s.insert_cost, MAX_NFA_ERRCNT);

        if( s.cost_bound < 1 || s.cost_bound > MAX_NFA_ERRCNT ) return ERR_PARAM;
        if( s.subst_cost < 1 ) return ERR_PARAM;
        if( s.delete_cost < 1 ) return ERR_PARAM;
        if( s.insert_cost < 1 ) return ERR_PARAM;

        // Don't allow 3 insertions or substitutions
        if( 3*s.subst_cost < s.cost_bound ) return ERR_PARAM;
        if( 3*s.insert_cost < s.cost_bound ) return ERR_PARAM;
      }
    break;
    default:
    break;
  }

  err = compile_regexp_thompson(&tnfa, ast);
  if( err ) return err;

  // convert it to the other kind.
  err = convert_thompson_nfa(&with_eps, &tnfa);
  free_thompson_nfa_description(&tnfa);
  if( err ) return err;

  err = remove_epsilon(&with_eps, nfa);
  free_nfa_description(&with_eps);

  nfa->settings = s;

  return err;
}

error_t compile_regexp_from_string(nfa_description_t* nfa, char* regexp) 
{
  error_t err;
  thompson_nfa_description_t tnfa;


  err = compile_regexp_from_string_thompson(&tnfa, regexp);
  if( err ) return err;

  // convert it to the other kind.
  err = convert_thompson_nfa(nfa, &tnfa);

  free_thompson_nfa_description(&tnfa);

  return err;
}
