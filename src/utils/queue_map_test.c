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

  femto/src/utils/queue_map_test.c
*/
#include "queue_map.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>


hash_t HashFunction1 (const void* foo);
int CompareFunction1 (const void* foo, const void* bar);
error_t ArrayToQueue (queue_map_t *q, hm_entry_t *theEntries, int numEntries);
error_t QueueEmptier (queue_map_t *q, hm_entry_t *theEntries, int numEntries);
error_t QueueRetrieve (queue_map_t *q, hm_entry_t *theEntries, int numEntries);
int PopMatch (hm_entry_t *theEntry, int i, hm_entry_t *theEntries);

int main (void)
{
  
  /*****************/
  /* BASIC TESTING */
  /*****************/
  {
    queue_map_t test1;
    error_t     error1;
    int         error2;
    hm_entry_t  entry1 = {"why is it like this?", "because michael said so!"};
    hm_entry_t  entry2;  //blank
    
    printf("\n\n");
    
    // test the create function
    error1 = queue_map_create ( &test1, HashFunction1, CompareFunction1 );
    assert (error1 == ERR_NOERR);
    
    // test the push function
    error1 = queue_map_push( &test1, &entry1 );
    assert (error1 == ERR_NOERR);
    
    // test the retrieve function
    error2 = queue_map_retrieve( &test1, &entry1 );
    assert (error2 == 1);
    
    // test the pop function
    error2 = queue_map_pop (&test1, &entry2);
    assert (error2 == 1);
    
    // test the destroy function
    queue_map_destroy( &test1 );
  }
  
  /*****************/
  /* BIG TEST NO.1 */
  /*****************/
  {
    int hm_num = 33;
    hm_entry_t ents[]={{"key1", "This "}, {"key2","is "}, {"key3","the "},
		       {"key4","song "}, {"key5","that "}, {"key6","doesn't "},
		       {"key7","end!  "}, {"key8","Yes "}, {"key9","it "},
		       {"key10","goes "}, {"key11","on "}, {"key12","and "},
		       {"key13","on "}, {"key14","my "}, {"key15","friend!  "},
		       {"key16","Some "}, {"key17","people "},
		       {"key18","started "}, {"key19","singing "},
		       {"key20","it "}, {"key21","not "}, {"key22","knowing "},
		       {"key23","what "}, {"key24","it "}, {"key25","was, "},
		       {"key26","and "}, {"key27","they'll "},
		       {"key28","continue "}, {"key29","singing "},
		       {"key30","it "}, {"key31","forever "},
		       {"key32","just "}, {"key33","because...  "}};

    // create variables to use
    queue_map_t theQueue;
    error_t     theError;
    
    // create the queue to be used
    theError = queue_map_create (&theQueue, HashFunction1, CompareFunction1);
    assert (theError == ERR_NOERR);
    
    // put all the entries in the queue_map
    theError = ArrayToQueue (&theQueue, ents, hm_num);
    assert (theError == ERR_NOERR);
    
    // retrieve all the entries in the queue_map
    theError = QueueRetrieve (&theQueue, ents, hm_num);
    assert (theError == ERR_NOERR);
    
    // pop all the entries in the queue_map
    theError = QueueEmptier (&theQueue, ents, hm_num);
    assert (theError == ERR_NOERR);
    
    // destroy the queue
    queue_map_destroy( &theQueue );
    
    
    //string compare the key with:
    // key+ i.string()
    //    strcmp(key, ents[i].key);
    
    
    
  }
  printf("All tests passed successfully!\n\n");
  return 0;
}



hash_t HashFunction1 (const void* foo) {
  return hash_string(foo);  }
int CompareFunction1 (const void* foo, const void* bar) {
  return strcmp(foo, bar);  }


// takes an array of hm_entry_t and
// adds them to the given queue
error_t ArrayToQueue (queue_map_t *q, hm_entry_t *theEntries, int numEntries)
{    
  error_t temp;
  
  for (int i = 0; i < numEntries; i++) {
    // add an entry to the queue
    temp = queue_map_push( q,  &theEntries[i] );
    if (temp != ERR_NOERR) {
      // if an error is found
      return temp;
    }
  }
  return ERR_NOERR;
}


// retrieves all of the entries
// in the queue and checks them
error_t QueueRetrieve (queue_map_t *q, hm_entry_t *theEntries, int numEntries)
{
  
  int error;
  hm_entry_t temp;
  
  for (int i = 0; i < numEntries; i++) {
    // make a copy of the entry to be searched for
    temp = theEntries[i];
    // search for the entry in the queue
    error = queue_map_retrieve( q,  &temp );
    if (error == 0) {
      // if an error is found
      return error;
    }
    
    error = PopMatch (&temp, i, theEntries);
    if (error == 0) {
      // if an error is found
      return error;
    }
    
  }
  return ERR_NOERR;
}


// pops all of the entries in the
// queue and checks them to be sure
error_t QueueEmptier (queue_map_t *q, hm_entry_t *theEntries, int numEntries)
{
  int theError;
  hm_entry_t theEntry;
  
  for (int i = 0; i < numEntries; i++) {
    // pop an entry from the queue
    theError = queue_map_pop( q,  &theEntry);
    if (theError == 0) {
      // if an error is found
      return ERR_INVALID;
    }
    // call the comparison function
    theError = PopMatch (&theEntry, numEntries-i-1, theEntries);
    if (theError == 0) {
      // if an error is found
      return ERR_INVALID;
    }
  }
  return ERR_NOERR;
}


// checks for a match with the given entry
// at cell 'i' in the given entry array
// returns a 1 if it matches, 0 if it doesn't
int PopMatch (hm_entry_t *theEntry, int i, hm_entry_t *theEntries)
{
  if ( (strcmp(theEntry->key, theEntries[i].key)) == 0) {
    if ( (strcmp(theEntry->value, theEntries[i].value)) == 0) {
      return 1;
    }
    printf("\nMatch not found for value: %s\n"
	   "Instead, found this value: %s\n\n",
	   (char*)theEntries[i].value, (char*)theEntry->value);
  }
    printf("\nMatch not found for key: %s\n"
	   "Instead, found this key: %s\n\n",
	   (char*)theEntries[i].key, (char*)theEntry->key);
  
  return 0;
}
