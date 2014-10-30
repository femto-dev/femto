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

  femto/src/main/flex_bison_parser.c
*/
#include <stdlib.h>
#include <stdio.h>
#include "ast.h"
#include "flex_bison_parser.h"

void yyerror(void* state, void* scanner, char const* s)
{
  fprintf(stderr, "%s\n", s);
}

struct ast_node* parse_string(int len, const char* data)
{
  parser_state_t pstate;
  int rc;

  memset(&pstate, 0, sizeof(parser_state_t));

  pstate.buf = data;
  pstate.len = len;
  pstate.pos = 0;
  pstate.errors = 0;

  yylex_init(&pstate.yyscanner);
  yyset_extra(&pstate, pstate.yyscanner);
  rc = yyparse(&pstate, pstate.yyscanner);

  yylex_destroy(pstate.yyscanner);

  if( rc != 0 ) { // 0 means success.
    if( pstate.root_node ) free_ast_node(pstate.root_node);
    return NULL;
  }

  return pstate.root_node;
}

