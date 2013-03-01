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

  femto/src/main/info_tool.c
*/
#include "index.h"
#include "femto_internal.h"

void usage(char* cmd)
{
  printf( "\nUsage: %s <index_path> [-s for summary]"
          "\n prints out important data about the index specified\n", cmd);
  exit(1);
}

int main ( int argc, char** argv )
{
  char* index_path = NULL;
  int summary = 0;
  
  for( int i = 1; i < argc; i++ ) {
    if( 0 == strcmp(argv[i], "-s") ||
        0 == strcmp(argv[i], "--summary" ) ) {
      summary = 1;
    } else if( argv[i][0] == '-' ) {
      usage(argv[0]);
    } else {
      index_path = argv[i];
    }
  }

  if( !index_path ) usage(argv[0]);


  path_translator_t trans;

  //data types
  data_block_t block;
  bucket_info_t buck;
  header_block_t h_block;
  index_locator_t locator;
  error_t err;

  //loop variables
  int i, j;

  //totals
  int64_t block_totals = 0;
  int64_t wavelet_totals = 0;
  int64_t occurence_totals = 0;
  int64_t mark_table_totals = 0;
  int64_t mark_array_totals = 0;
  int64_t chunk_totals = 0;
  double blocks_total = 0; //instead of casting, assign total to this variable

  //percentages
  double wavelet_per = -1;
  double occurence_per = -1;
  double mark_table_per = -1;
  double mark_array_per = -1;
  double chunk_per = -1;

  //sizes
  int64_t num_bytes_block = -1;
  int64_t num_bytes_bucket = -1;

  //header information
  int64_t h_number_of_blocks = -1;
  int64_t h_text_size        = -1;
  int64_t h_block_size       = -1;

  //block information
  int64_t num_buckets = -1;
  int64_t num_bytes_occs  = -1;

  //bucket information
  int64_t num_bytes_b_wtree = -1;
  int64_t num_bytes_b_mark_table  = -1;
  int64_t num_bytes_b_mark_arrays = -1;

  // chunk information
  int64_t num_bytes_chunk = -1;

  printf("   * Note a value of -1 means invalid \n");

  err = path_translator_init(&trans);
  die_if_err(err);

  err = path_translator_id_for_path(&trans, index_path, &locator);
  die_if_err(err);

  // do the header
  err = open_header_block( &h_block, &trans, locator);
  die_if_err(err);

  //Get the Information About the Header Block
  h_number_of_blocks = h_block.hdr.number_of_blocks;
  h_text_size = h_block.hdr.total_length;
  h_block_size = h_block.zbits.len;

  printf(" * INDEX INFORMATION\n"
	 "+-------------------------------\n"
	 "| Block Size:       %" PRIi64 "\n"
	 "| Bucket Size:      %" PRIi64"\n"
	 "| Mark Period:      %" PRIi64"\n"
	 "| Chunk Size:       %" PRIi64"\n"
	 "|\n"
	 "| Text Size:        %" PRIi64"\n"
	 "| Number of Docs:   %" PRIi64"\n"
	 "|\n"
	 "+-------------------------------\n\n\n",
         (int64_t) h_block.hdr.param.block_size,
         (int64_t) h_block.hdr.param.b_size,
         (int64_t) h_block.hdr.param.mark_period,
         (int64_t) h_block.hdr.param.chunk_size,
         h_text_size,
         h_block.hdr.number_of_documents );
	
  //Print the Information About the Header Block
  printf(" * HEADER BLOCK INFORMATION\n"
	 "+-------------------------------\n"
	 "| # of Data Blocks: %" PRIi64 "\n"
	 "|\n"
	 "| Header Block Size:%" PRIi64"\n"
	 "+-------------------------------\n\n\n",
	 h_number_of_blocks,
         (int64_t) h_block.zbits.len);

  err = close_header_block( &h_block);
  die_if_err(err);

  // done with header
  block_totals = h_block_size;


  // on to the data blocks
  for (i = 0; i < h_number_of_blocks; i++) {
    //Get the Information About the Block
    err = open_data_block( &block, &trans, locator, i, 1);
    die_if_err(err);

    num_bytes_block = block.zbits.len;
    num_buckets = block.hdr.num_buckets;
    num_bytes_occs = num_buckets * 4 * ALPHA_SIZE;

    if (summary == 0) {
      //Print the Information About the Block
      if (h_number_of_blocks != 1) {
	printf(" * INFORMATION ABOUT BLOCK #%d\n", i+1); }
      else {
	printf(" * INFORMATION ABOUT BLOCK\n"); }
      
      printf( "+------------------------------------\n"
	      "| Number of Buckets: %" PRIi64"\n"
	      "|\n"
	      "| Number of Bytes per Block: %" PRIi64"\n"
	      "| Size of Bucket Occurences: %" PRIi64"\n"
	      "+------------------------------------\n\n",
	      num_buckets, num_bytes_block, num_bytes_occs);
    }
    // this block is done
    block_totals += num_bytes_block;
    occurence_totals += num_bytes_occs;
      
    // buckets in this block
    for (j = 0; j < num_buckets; j++)  {
      //Get the Information About the Bucket
      err = get_bucket_info ( &buck, &block, j);
      die_if_err(err);
      
      num_bytes_b_wtree = buck.wavelet_tree_bytes;
      num_bytes_b_mark_table  = buck.leaf_mark_table_bytes;
      num_bytes_b_mark_arrays = buck.mark_array_bytes;
      num_bytes_chunk = buck.chunk_bytes;
      
      num_bytes_bucket = buck.total_bucket_size;

      if (summary == 0) {
	//Print the Information About the Bucket
      	if (num_buckets != 1) {
	  printf("    * INFORMATION ABOUT BUCKET #%d\n",j+1); }
	else {
	  printf("    * INFORMATION ABOUT BUCKET\n"); }
	
	printf( "   +---------------------------------------\n"
		"   | Total Bucket Size:  %" PRIi64"\n"
		"   |\n"
		"   | Number of Wavelet Tree Bytes: %" PRIi64"\n"
		"   | Number of Mark Table Bytes:   %" PRIi64"\n"
		"   | Number of Mark Array Bytes:   %" PRIi64"\n"
		"   | Number of Chunk Bytes Bytes:  %" PRIi64"\n"
		"   +---------------------------------------\n\n",
		num_bytes_bucket, num_bytes_b_wtree,
		num_bytes_b_mark_table, num_bytes_b_mark_arrays,
                num_bytes_chunk );
      }
      // this bucket is done
      wavelet_totals += num_bytes_b_wtree;
      mark_table_totals += num_bytes_b_mark_table;
      mark_array_totals += num_bytes_b_mark_arrays;
      chunk_totals += num_bytes_chunk;
      
    }
    
    close_data_block( &block);
    if (summary == 0) {
      printf("\n");
    }
  }
  
  // calculate some totals
  blocks_total = block_totals;
  wavelet_per = (wavelet_totals / blocks_total) * 100;
  occurence_per = (occurence_totals / blocks_total) * 100;
  mark_table_per = (mark_table_totals / blocks_total) * 100;
  mark_array_per = (mark_array_totals / blocks_total) * 100;
  chunk_per = (chunk_totals / blocks_total) * 100;

  // print the summary
  printf("  || SIZE TOTALS\n"
	 "  ||===================================\n"
	 "  || Total Size:  %20" PRIi64"\n"
	 "  ||\n"
	 "  || Wavelets:    %20" PRIi64"\n"
	 "  || Occurences:  %20" PRIi64"\n"
	 "  || Mark Tables: %20" PRIi64"\n"
	 "  || Mark Arrays: %20" PRIi64"\n"
	 "  || Doc Chunks:  %20" PRIi64"\n"
	 "  ||===================================\n\n",
	 block_totals, wavelet_totals, occurence_totals,
	 mark_table_totals, mark_array_totals, chunk_totals);

  printf("  || SIZE PERCENTAGES \n"
	 "  ||===============================\n"
	 "  || Wavelets:    %14.10f %%\n"
	 "  || Occurences:  %14.10f %%\n"
	 "  || Mark Tables: %14.10f %%\n"
	 "  || Mark Arrays: %14.10f %%\n"
	 "  || Doc Chunks:  %14.10f %%\n"
	 "  ||===============================\n\n\n",
	 wavelet_per, occurence_per, mark_table_per, mark_array_per, chunk_per);

  path_translator_destroy(&trans);

  return 0;
}
















