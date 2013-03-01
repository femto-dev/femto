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

  femto/src/main/results_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "index_types.h"
#include "results.h"
#include "util.h"

typedef enum {
  BOOL_AND = 1,
  BOOL_OR,
  BOOL_NOT,
  BOOL_THEN,
  BOOL_WITHIN,
  BOOL_OR_DOC //Used to make testing methods efficient code-wise
} boolTest;

struct boolData
{
  int64_t doc;
  int64_t offset;
};

void test_intersectResults(results_t* left,
                           results_t* right,
                           results_t* valid,
                           int lenValid,
                           boolTest testType);
                           
void test_unionResults(results_t* left,
                       results_t* right,
                       results_t* valid,
                       int lenValid,
                       boolTest testType);
                       
void test_subtractResults(results_t* left,
                          results_t* right,
                          results_t* valid,
                          int lenValid,
                          boolTest testType);
                          
void test_thenResults(results_t* left,
                      results_t* right,
                      int distance,
                      results_t* valid,
                      int lenValid,
                      boolTest testType);
                      
void test_withinResults(results_t* left,
                        results_t* right,
                        int distance,
                        results_t* valid,
                        int lenValid,
                        boolTest testType);


void test_booleanFunctions(int lenLeft, 
                           struct boolData* listLeft,
                           int lenRight,
                           struct boolData* listRight,
                           int distance,
                           int lenValid,
                           struct boolData* listValid,
                           boolTest testType)
{
  error_t error;
  results_writer_t leftWriter;
  results_writer_t rightWriter;
  results_writer_t validWriter;
  results_t leftResults = empty_results(0);
  results_t rightResults = empty_results(0);
  results_t validResults = empty_results(0);
  if(testType == BOOL_THEN || testType == BOOL_WITHIN || testType == BOOL_OR_DOC)
  {
    error = results_writer_create(&leftWriter,RESULT_TYPE_DOC_OFFSETS);
    die_if_err(error);
    error = results_writer_create(&rightWriter,RESULT_TYPE_DOC_OFFSETS);
    die_if_err(error);
    error = results_writer_create(&validWriter,RESULT_TYPE_DOC_OFFSETS);
    die_if_err(error);
  }
  else
  {
    error = results_writer_create(&leftWriter,RESULT_TYPE_DOCUMENTS);
    die_if_err(error);
    error = results_writer_create(&rightWriter,RESULT_TYPE_DOCUMENTS);
    die_if_err(error);
    error = results_writer_create(&validWriter,RESULT_TYPE_DOCUMENTS);
    die_if_err(error);
  }
  for( int i = 0; i < lenLeft; i++ ) 
  {
    error = results_writer_append(&leftWriter, listLeft[i].doc, listLeft[i].offset);
    die_if_err(error);
  }
  error = results_writer_finish(&leftWriter, &leftResults);
  die_if_err(error);
  for( int i = 0; i < lenRight; i++ ) 
  {
    error = results_writer_append(&rightWriter, listRight[i].doc, listRight[i].offset);
    die_if_err(error);
  }
  error = results_writer_finish(&rightWriter, &rightResults);
  die_if_err(error);

  for( int i = 0; i < lenValid; i++ ) 
  {
    error = results_writer_append(&validWriter, listValid[i].doc, listValid[i].offset);
    die_if_err(error);
  }
  error = results_writer_finish(&validWriter, &validResults);
  die_if_err(error);
  
  results_writer_destroy(&leftWriter);
  results_writer_destroy(&rightWriter);
  results_writer_destroy(&validWriter);

  switch(testType)
  {
    case BOOL_AND:
    {
      test_intersectResults(&leftResults, &rightResults, &validResults, lenValid, testType);\
      break;
    }
    case BOOL_OR:
    {
      test_unionResults(&leftResults, &rightResults, &validResults, lenValid, testType);
      break;
    }
    case BOOL_OR_DOC:
    {
      test_unionResults(&leftResults, &rightResults, &validResults, lenValid, testType);
      break;
    }
    case BOOL_NOT:
    {
      test_subtractResults(&leftResults, &rightResults, &validResults, lenValid, testType);
      break;
    }
    case BOOL_THEN:
    {
      test_thenResults(&leftResults, &rightResults, distance, &validResults, lenValid, testType);
      break;
    }
    case BOOL_WITHIN:
    {
      test_withinResults(&leftResults, &rightResults, distance, &validResults, lenValid, testType);
      break;
    }
  }

  results_destroy(&leftResults);
  results_destroy(&rightResults);
  results_destroy(&validResults);
  
}

void test_checkResults(results_t* funcResults,
                       results_t* valid,
                       int lenValid,
                       boolTest testType)
{
  error_t error;
  results_reader_t funcResultsReader;
  results_reader_t validResultsReader;
  int64_t nextFuncDocResult;
  int64_t nextValidDocResult;
  int64_t nextFuncOffsetResult;
  int64_t nextValidOffsetResult;
  int funcRead;
  int validRead;
  
  error = results_reader_create(&funcResultsReader, funcResults);
  die_if_err(error);
  error = results_reader_create(&validResultsReader, valid);
  die_if_err(error);
  
  if(testType == BOOL_THEN || testType == BOOL_WITHIN || testType == BOOL_OR_DOC)
  {
    funcRead = results_reader_next(&funcResultsReader, 
                                   &nextFuncDocResult, 
                                   &nextFuncOffsetResult);
    validRead = results_reader_next(&validResultsReader, 
                                    &nextValidDocResult, 
                                    &nextValidOffsetResult);
  }
  else
  {
    funcRead = results_reader_next(&funcResultsReader, 
                                   &nextFuncDocResult, 
                                   NULL);
    validRead = results_reader_next(&validResultsReader, 
                                    &nextValidDocResult, 
                                    NULL);
  }
  
  if(DEBUG) printf("Checking contents of results..\n");
  for(int i = 0; i < lenValid; i++)
  {
    assert(funcRead);
    assert(validRead);
    
    if(testType == BOOL_THEN || testType == BOOL_WITHIN || testType == BOOL_OR_DOC)
    {

      funcRead = results_reader_next(&funcResultsReader, 
                                     &nextFuncDocResult, 
                                     &nextFuncOffsetResult);
      validRead = results_reader_next(&validResultsReader, 
                                      &nextValidDocResult, 
                                      &nextValidOffsetResult);
      assert(nextFuncDocResult == nextValidDocResult);
      assert(nextFuncOffsetResult == nextValidOffsetResult);
    }
    else
    {
      funcRead = results_reader_next(&funcResultsReader, 
                                     &nextFuncDocResult, 
                                     NULL);
      validRead = results_reader_next(&validResultsReader, 
                                      &nextValidDocResult, 
                                      NULL);
      assert(nextFuncDocResult == nextValidDocResult);
    }
    
    
  }
  
  if(DEBUG) printf("Contents match.\n");
  
  if(DEBUG) printf("Cleaning up test..\n");
  
  results_reader_destroy(&funcResultsReader);
  results_reader_destroy(&validResultsReader);
  results_destroy(funcResults);
  results_destroy(valid);
  
  if(DEBUG) printf("Clean-up finished.\n");
  
  if(DEBUG) printf("Test passed.\n");
}

void test_intersectResults(results_t* left,
                           results_t* right,
                           results_t* valid,
                           int lenValid,
                           boolTest testType)
{
  error_t error;
  results_t funcResults = empty_results(0);
    
  if(DEBUG) printf("Testing Boolean AND Function..\n");
  
  error = intersectResults(left, right, &funcResults);
  die_if_err(error);
  
  if(DEBUG) printf("Finished executing Boolean AND Function, ");
  if(DEBUG) printf("cleaning up execution and checking results..\n");
  
  test_checkResults(&funcResults, valid, lenValid, testType);

  results_destroy(&funcResults);
  
}

void test_unionResults(results_t* left,
                       results_t* right,
                       results_t* valid,
                       int lenValid,
                       boolTest testType)
{
  error_t error;
  results_t funcResults = empty_results(0);
  if(left->type == RESULT_TYPE_DOC_OFFSETS)
  {
    if(DEBUG) printf("Testing Boolean OR Function with Offsets enabled..\n");
  }
  else
  {
    if(DEBUG) printf("Testing Boolean OR Function..\n");
  }
  
  error = unionResults(left, right, &funcResults);
  die_if_err(error);
  
  if(DEBUG) printf("Finished executing Boolean OR Function, ");
  if(DEBUG) printf("cleaning up execution and checking results..\n");
  
  test_checkResults(&funcResults, valid, lenValid, testType);

  results_destroy(&funcResults);
}

void test_subtractResults(results_t* left,
                          results_t* right,
                          results_t* valid,
                          int lenValid,
                          boolTest testType)
{
  error_t error;
  results_t funcResults = empty_results(0);
    
  if(DEBUG) printf("Testing Boolean NOT Function..\n");
  
  error = subtractResults(left, right, &funcResults);
  die_if_err(error);
  
  if(DEBUG) printf("Finished executing Boolean NOT Function, ");
  if(DEBUG) printf("cleaning up execution and checking results..\n");
  
  test_checkResults(&funcResults, valid, lenValid, testType);

  results_destroy(&funcResults);
}

void test_thenResults(results_t* left,
                      results_t* right,
                      int distance,
                      results_t* valid,
                      int lenValid,
                      boolTest testType)
{
  error_t error;
  results_t funcResults = empty_results(0);
    
  if(DEBUG) printf("Testing Boolean THEN Function..\n");
  
  error = thenResults(left, right, &funcResults, distance);
  die_if_err(error);
  
  if(DEBUG) printf("Finished executing Boolean THEN Function, ");
  if(DEBUG) printf("cleaning up execution and checking results..\n");

  test_checkResults(&funcResults, valid, lenValid, testType);

  results_destroy(&funcResults);
}

void test_withinResults(results_t* left,
                        results_t* right,
                        int distance,
                        results_t* valid,
                        int lenValid,
                        boolTest testType)
{
  error_t error;
  results_t funcResults = empty_results(0);

  if(DEBUG) printf("Testing Boolean WITHIN Function..\n");
  
  error = withinResults(left, right, &funcResults, distance);
  die_if_err(error);
  
  if(DEBUG) printf("Finished executing Boolean WITHIN Function, ");
  if(DEBUG) printf("cleaning up execution and checking results..\n");
  
  test_checkResults(&funcResults, valid, lenValid, testType);

  results_destroy(&funcResults);
}


void test_writer_reader_doc_offs(int ndocs, int64_t* docs,
                                 int* noccs, int64_t** offs, int neg)
{
  results_writer_t writer;
  results_t results = empty_results(0);
  results_reader_t reader;
  error_t err;
  int64_t doc, off;
  int ret;

  if( neg ) {
    err = results_writer_create(&writer, RESULT_TYPE_DOC_OFFSETS | RESULT_TYPE_REV_OFFSETS);
  } else {
    err = results_writer_create(&writer, RESULT_TYPE_DOC_OFFSETS);
  }
  die_if_err(err);

  for( int i = 0; i < ndocs; i++ ) {
    for( int j = 0; j < noccs[i]; j++ ) {
      err = results_writer_append(&writer, docs[i], offs[i][j]);
      die_if_err(err);
    }
  }

  err = results_writer_finish(&writer, &results);
  die_if_err(err);

  results_writer_destroy(&writer);

  // now try reading them!
  err = results_reader_create(&reader, &results);
  die_if_err(err);

  for( int i = 0; i < ndocs; i++ ) {
    for( int j = 0; j < noccs[i]; j++ ) {
      off = doc = 0;
      ret = results_reader_next(&reader, &doc, &off);
      assert( ret == 1 );
      assert( doc == docs[i] );
      assert( off == offs[i][j] );
    }
  }

  ret = results_reader_next(&reader, &doc, &off);
  assert( ret == 0 );

  results_reader_destroy(&reader);

  // now try reading them!
  err = results_reader_create(&reader, &results);
  die_if_err(err);

  for( int i = 0; i < ndocs; i++ ) {
    ret = results_reader_next(&reader, &doc, NULL);
    assert( ret == 1 );
    assert( doc == docs[i] );
  }

  ret = results_reader_next(&reader, &doc, NULL);
  assert( ret == 0 );

  results_reader_destroy(&reader);

  results_destroy(&results);
}


void test_writer_reader(int len, int64_t* list)
{
  results_writer_t writer;
  results_t results = empty_results(0);
  results_reader_t reader;
  error_t err;
  int64_t temp;
  int ret;

  err = results_writer_create(&writer, RESULT_TYPE_DOCUMENTS);
  die_if_err(err);

  for( int i = 0; i < len; i++ ) {
    err = results_writer_append(&writer, list[i], 0);
    die_if_err(err);
  }

  err = results_writer_finish(&writer, &results);
  die_if_err(err);

  results_writer_destroy(&writer);

  // now try reading them!
  err = results_reader_create(&reader, &results);
  die_if_err(err);

  for( int i = 0; i < len; i++ ) {
    ret = results_reader_next(&reader, &temp, NULL);
    assert( ret == 1 );
    assert( temp == list[i] );
  }

  ret = results_reader_next(&reader, &temp, NULL);
  assert( ret == 0 );

  results_reader_destroy(&reader);

  results_destroy(&results);

}

void test_random_results(int len)
{
  int64_t* list = malloc(len*sizeof(int64_t));

  if(DEBUG) printf("Generating long pattern %i\n", len);
  for( int i = 0; i < len; i++ ) {
    list[i] = rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    list[i] <<= 7;
    list[i] ^= rand();
    // force the result to be positive
    list[i] &= 0x7fffffffffffffff;
  }
  if(DEBUG) printf("Sorting long pattern %i\n", len);
  len = sort_dedup(list, len, sizeof(int64_t), compare_int64);
  for( int i = 1; i < len; i++ ) {
    assert( list[i-1] < list[i] );
  }
  if(DEBUG) printf("new length is %i\n", len);
  if(DEBUG) printf("Testing reader/writer %i\n", len);
  test_writer_reader(len, list);

  free(list);
}

int main(int argc, char** argv)
{

  // try creating some data to try.
  {
    int64_t list[] = {0,3,5,12};
    int len = 4;
    test_writer_reader(len, list);
  }
  {
    // test with document offsets
    int ndocs = 4;
    int64_t docs[] = {0,7,9,20};
    int noccs[] = {3,1,2,1};
    int64_t offs1[] = {0,1,2};
    int64_t offs2[] = {0};
    int64_t offs3[] = {17,18};
    int64_t offs4[] = {99};
    int64_t* offs[] = {offs1, offs2, offs3, offs4};

    test_writer_reader_doc_offs(1, docs, noccs, offs, 0);
    test_writer_reader_doc_offs(ndocs, docs, noccs, offs, 0);
  }
  {
    // test with document and negative offsets
    // test with document offsets
    int ndocs = 4;
    int64_t docs[] = {0,7,9,20};
    int noccs[] = {3,1,2,1};
    int64_t offs1[] = {-2, -1, 0};
    int64_t offs2[] = {0};
    int64_t offs3[] = {-18, -17};
    int64_t offs4[] = {-99};
    int64_t* offs[] = {offs1, offs2, offs3, offs4};

    test_writer_reader_doc_offs(1, docs, noccs, offs, 1);
    test_writer_reader_doc_offs(ndocs, docs, noccs, offs, 1);
  }
  {
    //Test document based boolean AND function
    struct boolData emptyList[] = {};
    struct boolData list1[] = {{1,0},{2,0},{3,0},{4,0},{5,0},{6,0},{7,0},{8,0}};
    struct boolData list2[] = {{0,0},{3,0},{5,0},{12,0}};
    struct boolData valid[] = {{3,0},{5,0}};
    test_booleanFunctions(8,list1,4,list2,0,2,valid,BOOL_AND);
    
    test_booleanFunctions(8,list1,0,emptyList,0,0,emptyList,BOOL_AND);
    test_booleanFunctions(0,emptyList,8,list1,0,0,emptyList,BOOL_AND);
    test_booleanFunctions(0,emptyList,0,emptyList,0,0,emptyList,BOOL_AND);
  }
  {
    //Test document based boolean OR function
    struct boolData emptyList[] = {};
    struct boolData list1[] = {{1,0},{2,0},{3,0},{4,0},{5,0},{6,0},{7,0},{8,0}};
    struct boolData list2[] = {{0,0},{3,0},{5,0},{12,0}};
    struct boolData valid[] = {{0,0},{1,0},{2,0},{3,0},{4,0},{5,0},{6,0},{7,0},{8,0},{12,0}};
    test_booleanFunctions(8,list1,4,list2,0,10,valid,BOOL_OR);
    
    test_booleanFunctions(8,list1,0,emptyList,0,8,list1,BOOL_OR);
    
    test_booleanFunctions(0,emptyList,4,list2,0,4,list2,BOOL_OR);
    test_booleanFunctions(0,emptyList,0,emptyList,0,0,emptyList,BOOL_OR);
  }
  {
    //Test offset based boolean OR function
    if(DEBUG) printf("MIKE'S OR\n");
    struct boolData emptyList[] = {};
    struct boolData list1[] = {{1,1},{2,2},{3,3},{4,4},{5,5},{6,6},{7,7},{8,8}};
    struct boolData list2[] = {{0,0},{3,1},{5,1},{12,1}};
    struct boolData valid[] = {{0,0},{1,1},{2,2},{3,1},{3,3},{4,4},{5,1},{5,5},{6,6},{7,7},{8,8},{12,1}};
    test_booleanFunctions(8,list1,4,list2,0,12,valid,BOOL_OR_DOC);
    
    test_booleanFunctions(8,list1,0,emptyList,0,8,list1,BOOL_OR_DOC);
    
    test_booleanFunctions(0,emptyList,4,list2,0,4,list2,BOOL_OR_DOC);
  }
  {
    //Test document based boolean NOT function
    struct boolData emptyList[] = {};
    struct boolData list1[] = {{1,0},{2,0},{3,0},{4,0},{5,0},{6,0},{7,0},{8,0}};
    struct boolData list2[] = {{0,0},{3,0},{5,0},{12,0}};
    struct boolData valid[] = {{1,0},{2,0},{4,0},{6,0},{7,0},{8,0}};
    test_booleanFunctions(8,list1,4,list2,0,6,valid,BOOL_NOT);
    
    test_booleanFunctions(8,list1,0,emptyList,0,8,list1,BOOL_NOT);
    
    test_booleanFunctions(0,emptyList,4,list2,0,0,emptyList,BOOL_NOT);
    
    test_booleanFunctions(0,emptyList,0,emptyList,0,0,emptyList,BOOL_NOT);
  }
  {
    //Test document & offset based boolean THEN function
    struct boolData emptyList[] = {};
    struct boolData list1[] = {{1,5},{1,12},{2,4},{2,14},{3,1},{4,4}};
    struct boolData list2[] = {{0,0},{1,2},{1,6},{1,10},{2,1},{2,6},{2,10},{4,5}};
    struct boolData list3[] = {{0,0},{1,2},{1,6},{1,10},{2,1},{2,3},{2,10},{4,5}};
    struct boolData valid1[] = {{1,5},{2,4},{4,4}};
    struct boolData valid2[] = {{1,10},{2,3}};
    test_booleanFunctions(6,list1,8,list2,2,3,valid1,BOOL_THEN);
    
    test_booleanFunctions(6,list1,8,list3,-2,2,valid2,BOOL_THEN);
    
    test_booleanFunctions(6,list1,0,emptyList,2,0,emptyList,BOOL_THEN);
    
    test_booleanFunctions(0,emptyList,8,list2,2,0,emptyList,BOOL_THEN);
    
    test_booleanFunctions(0,emptyList,0,emptyList,2,0,emptyList,BOOL_THEN);
  }
  {
    //Test document & offset based boolean WITHIN function
    struct boolData emptyList[] = {};
    struct boolData list1[] = {{1,5},{1,12},{2,4},{2,14},{3,1},{4,4}};
    struct boolData list2[] = {{0,0},{1,2},{1,6},{1,10},{2,1},{2,6},{2,10},{4,5}};
    struct boolData valid1[] = {{1,2},{1,5},{1,10},{2,1},{2,4},{4,4}};
    test_booleanFunctions(6,list1,8,list2,3,6,valid1,BOOL_WITHIN);
      
    test_booleanFunctions(6,list1,8,list2,-3,6,valid1,BOOL_WITHIN);
      
    test_booleanFunctions(6,list1,0,emptyList,2,0,emptyList,BOOL_WITHIN);
      
    test_booleanFunctions(0,emptyList,8,list2,2,0,emptyList,BOOL_WITHIN);
  
    test_booleanFunctions(0,emptyList,0,emptyList,2,0,emptyList,BOOL_WITHIN);
  }
  

  
  
  test_random_results(1);
  test_random_results(5);
  test_random_results(50);
  test_random_results(100);
  test_random_results(1000);
  test_random_results(129639);
  test_random_results(1054892);
  test_random_results(10540892);
  printf("results tests PASSED\n");
  return 0;
}
