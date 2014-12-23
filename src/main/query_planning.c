/*
  (*) 2006-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/query_planning.c
*/
#include "query_planning.h"

void streamline_query(struct ast_node* node)
{
  if( DEBUG > 2 ) {
    printf("Before streamlining\n");
    print_ast_node(stdout, node, 0);
  }

  switch(node->type)
  {
    case AST_NODE_BOOL:
    { 
      struct boolean_node* bnode = (struct boolean_node*)node;
      streamline_query(bnode->left);
      streamline_query(bnode->right);
      break;
    }
    case AST_NODE_REGEXP:
    {
      struct regexp_node* rnode = (struct regexp_node*)node;
      //Static num so that we go through the proper number of sequences
      int num = rnode->choices.num;
      for(int i = 0; i < num; i++)
      {
        fix_initial(rnode->choices.list[i]);
        if(rnode->choices.num != 0)
        {
          fix_final(rnode->choices.list[i]);
        }
      }
      break;
    }
    default:
      break;
  }

  if( DEBUG > 2 ) {
    printf("After streamlining\n");
    print_ast_node(stdout, node, 0);
  }
}

int matches_empty_string(struct ast_node* node)
{
  switch(node->type)
  {
    case AST_NODE_ATOM:
    {
      struct atom_node* anode = (struct atom_node*)node;
      if(anode->repeat.min == 0)
      {
        return 1;
      }
      else if(anode->child->type == AST_NODE_REGEXP)
      {
        return matches_empty_string(anode->child);
      } // otherwise, it was a char, a set, or a string.
      return 0;
      break;
    }
    case AST_NODE_REGEXP:
    {
      struct regexp_node* rnode = (struct regexp_node*)node;
      for(int i = 0; i < rnode->choices.num; i++)
      {
        if(matches_empty_string(rnode->choices.list[i]))
        {
          return 1;
        }
      }
      return 0;
      break;
    }
    case AST_NODE_SEQUENCE:
    {
      struct sequence_node* snode = (struct sequence_node*)node;
      for(int i = 0; i < snode->atoms.num; i++)
      {
        if( 0 == matches_empty_string(snode->atoms.list[i]) )
        {
          //No Match
          return 0;
        }
      }
      //Matches empty, AND its the last one, the sequence matches empty
      //string
      return 1;
      break;
    }
    default:
      assert(0);
      break;
  }
  return 0;
}


void fix_initial(struct ast_node* node)
{
  switch(node->type)
  {
    case AST_NODE_REGEXP:
    {
      struct regexp_node* rnode = (struct regexp_node*)node;
      for(int i = 0; i< rnode->choices.num; i++)
      {
        fix_initial(rnode->choices.list[i]);
      }
      break;
    }
    case AST_NODE_SEQUENCE:
    {
      struct sequence_node* snode = (struct sequence_node*)node;
      if( snode->atoms.num != 0 )
      {
        if (matches_empty_string(snode->atoms.list[0]) )
        {
          remove_node_list(&snode->atoms, 0);
          fix_initial(node);
        }
        else
        {
          struct atom_node* anode = (struct atom_node*)snode->atoms.list[0];
          if(anode->repeat.max > anode->repeat.min)
          {
            anode->repeat.max = anode->repeat.min;
          }
          fix_initial(snode->atoms.list[0]);
        }
      }
      break;
    }
    case AST_NODE_ATOM:
    {
      struct atom_node* anode = (struct atom_node*)node;
      if(anode->child->type == AST_NODE_REGEXP)
      {
        fix_initial(anode->child);
      }
      break;
    }
    default:
      break;
  }
}

void fix_final(struct ast_node* node)
{
  int location;
  switch(node->type)
  {
    case AST_NODE_REGEXP:
    {
      struct regexp_node* rnode = (struct regexp_node*)node;
      for(int i = rnode->choices.num - 1; i > 0; i--)
      {
        fix_final(rnode->choices.list[i]);
      }
      break;
    }
    case AST_NODE_SEQUENCE:
    {
      struct sequence_node* snode = (struct sequence_node*)node;
      if( snode->atoms.num != 0 )
      {
        location = snode->atoms.num - 1;
        if (matches_empty_string(snode->atoms.list[location]) )
        {
          remove_node_list(&snode->atoms, location);
          fix_final(node);
        }
        else
        {
          struct atom_node* anode = (struct atom_node*)snode->atoms.list[location];
          if(anode->repeat.max > anode->repeat.min)
          {
            anode->repeat.max = anode->repeat.min;
          }
          fix_final(snode->atoms.list[location]);
        }
      }
      break;
    }
    case AST_NODE_ATOM:
    {
      struct atom_node* anode = (struct atom_node*)node;
      if(anode->child->type == AST_NODE_REGEXP)
      {
        fix_final(anode->child);
      }
      break;
    }
    default:
      break;
  }
}

