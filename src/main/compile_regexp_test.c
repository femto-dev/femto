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

  femto/src/main/compile_regexp_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "compile_regexp.h"


int main (void)
{
  error_t err;
  thompson_nfa_description_t tnfa;
  nfa_description_t nfa;
  nfa_description_t nfa2;
  struct ast_node* root;

  root = parse_file(stdin);

  printf("AST:\n");
  print_ast_node(stdout, root, 0);
  err = compile_regexp_thompson(&tnfa, root);
  die_if_err(err);

  free_ast_node(root);

  printf("THOMPSON NFA:\n");
  print_thompson_nfa(stdout, &tnfa);

  err = convert_thompson_nfa(&nfa, &tnfa);
  die_if_err( err );

  printf("CONVERTED TO NFA:\n");
  print_nfa(stdout, &nfa);

  printf("AFTER REMOVING EPLISONS:\n");
  remove_epsilon(&nfa, &nfa2);
  print_nfa(stdout, &nfa2);
  
  printf("AFTER COLLAPSING:\n");
  collapse_nfa(&nfa2);
  print_nfa(stdout, &nfa2);

  free_nfa_description(&nfa2);

  free_nfa_description(&nfa);

  free_thompson_nfa_description(&tnfa);

  return 0;
}

