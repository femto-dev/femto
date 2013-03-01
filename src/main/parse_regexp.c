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

  femto/src/main/parse_regexp.c
*/
#include <stdlib.h>
#include <stdio.h>
#include "ast.h"


int main (void)
{
  struct ast_node* root;

  root = parse_file(stdin);


  print_ast_node(stdout, root, 0);

  {
    char* pat = ast_to_string(root);
    printf("Parsed pattern: '%s'\n", pat);
    free(pat);
  }

  printf("case-insensitive version:\n");

  icase_ast(&root);

  print_ast_node(stdout, root, 0);

  {
    char* pat = ast_to_string(root);
    printf("Parsed pattern: '%s'\n", pat);
    free(pat);
  }

 
  return 0;
}

