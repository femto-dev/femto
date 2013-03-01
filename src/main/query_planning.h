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

  femto/src/main/query_planning.h
*/
#ifndef _QUERY_PLANNING_H_
#define _QUERY_PLANNING_H_

#include <stdio.h>
#include "error.h"
#include "ast.h"
#include <assert.h>

void streamline_query(struct ast_node* node);
void fix_initial(struct ast_node* node);
void fix_final(struct ast_node* node);
int matches_empty_string(struct ast_node* node);

#endif
