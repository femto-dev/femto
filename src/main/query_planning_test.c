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

  femto/src/main/query_planning_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include "ast.h"
#include <string.h>
#include "query_planning.h"

void testQueryPlanning(void)
{
  printf("Starting Query Planning Tests..\n");
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "a*(b|b?bb?)c";
    char* valid = "(b|bb?)c";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 1 Passed.\n");
    }
    else
    {
      printf("Test 1 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "a*(bc|d)+";
    char* valid = "(bc|d)";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 2 Passed.\n");
    }
    else
    {
      printf("Test 2 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "a*(bc|d)b?";
    char* valid = "(bc|d)";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 3 Passed.\n");
    }
    else
    {
      printf("Test 3 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "a?(b*cd|e?fg?)h";
    char* valid = "(cd|fg?)h";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 4 Passed.\n");
    }
    else
    {
      printf("Test 4 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "a*b?cd(e*|f)g?";
    char* valid = "cd";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 5 Passed.\n");
    }
    else
    {
      printf("Test 5 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "a+bc+";
    char* valid = "abc";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 6 Passed.\n");
    }
    else
    {
      printf("Test 6 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
  {
    struct ast_node* result;
    char* stringResult;
    char* query1 = "ab*c";
    char* valid = "ab*c";
    result = parse_string(strlen(query1),query1);
    streamline_query(result);
    stringResult = ast_to_string(result, 0);
    if( 0 == strcmp(stringResult,valid) )
    {
      printf("Test 7 Passed.\n");
    }
    else
    {
      printf("Test 7 Failed: %s compared to %s\n", stringResult,valid);
      exit(1);
    }
    free(stringResult);
    free_ast_node(result);
  }
}

int main(int argc, char** argv)
{
  testQueryPlanning();
  printf("All Query Planning Tests Executed.\n");
}
