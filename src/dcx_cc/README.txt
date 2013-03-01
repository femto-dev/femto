A description of the algorithm. This is a parallel, external-memory
DCX variant. Pipelining is not actually necessary, except to
cover communication and IO latency. Since all of the distributions
are explicit, pipelining need not be used to prevent redundant writes
to disk. We expect to use pipes in the following settings:
 - pipes must be used to communicate to the (procsses owning the) bin
 - pipes must be used to do file I/O
 - thus the distribute and sorting steps must take pipes as input and output.
 - to do permutations and bucket sorting on external memory, we will need
   to support read-modify-write I/O. We do this with mmap/madvise.

COULD DO:
  -- implement improvements from  "Performance of Linear Time Suffix Sorting Algorithms":
    -- 3.1 shorten the recursion string - we can leave out all unique characters
after a first unique character.
       To do this, store a bit in each (offset,rank) to indicate a unique rank?
       Then when preparing input for the recursion, do not put the
       unique ones after the first unique one. This would be done in the
       naming node. But we would have already transmitted the removeable
       records... Trouble is, it would change the offsets? Then we would have
       to recover the original offsets knowing which ones were unique. In 
       their original description, they do this shortening only when there
       are many characters in this situation, and to do it they must store
       extra information.
  # ADDING LCP array: after Karkkainen (Lin. Work. SA Constr. pg 11).
  # k = SA[i] and j = SA[i+1] then find l such that k+l and j+l are
  # both in the sample. If T[k,k+l) != T[j,j+l) then lcp[i] can be
  # computed locally. Otherwise, lcp[i] = l + lcp(S_k+i, S_j+l). 
  # The lcp of S_k+l and S_j+l can be approximated within an additive
  # term v from the lcp information of the recursive string R using range
  # minima queries.
 - fix the bucket sorter to take pipe input/output
 - remove redundant reads
 - Go directly to forming tuples from the input.
   (Not going to, because it's not as big a deal anymore
    now that records are compressed, and because doing so
    would disrupt forming the BWT).

NOTES:
 - bins have Period character overlap
 - ranks always start at 1
 - input for recursion has 0 characters dividing different sample sets.

ALGORITHM OUTLINE:

suffix_sort:

  form_tuples
    Given (in offset bins):
      input (offset,name), where name is really a 'character' here.
    Produce (in character bins, by first character):
      tuples (offset, characters[Period])

     Finish permuting the input bin by offset. Then create tuple records,
     i.e. (0,T[0..p]),(1,T[1..p+1]),(2,T[2..p+2]). Distribute these tuple
     records into rank bins.

  sort_and_name
    Given (in character bins):
      tuples (offset,characters[Period])
    Produce (in offset bins)
      sample_r (offset,name) for recursion
      sample (offset,name) for use after recursion
      nonsample (offset,name) for use after recursion

    Sort the tuple records in each bin, and then go through that bin
    recording the 'name' for each record. Note that whether or not a 
    'name' is unique must also be recorded in the output. In addition,
    this routine checks to see if the tuples in the bin are unique.

    Distribute named tuples to the offset bins in three sets:
      sample suffix (name,offset) prepared for recursion,
      sample suffix (name,offset) for use coming out of recursion, and
      nonsample suffix (name,offset) for use in form-merge-records.

  maybe_recurse
    Given (in offset bins):
      sample_r (offset,name) prepared for recursion
    Produce (in offset bins):
      sample_ranks (offset,rank) from recursion

    If the tuples from sort_and_name in all the bins are not unique, recurse
    -> if we recurse: 
         Call the recursion with the input prepared for it
         After the recursion, we have (offset,rank) in offset bins.
    -> if we do not recurse:
         output_unique
           Translate the (name,offset) sample and nonsample records to
           (rank,offset) records and distribute into offset bins.

  form_merge_records
    Given (in offset bins):
      sample (offset,rank) from recursion in offset bins
      sample (offset,name) from naming in offset bins
      nonsample (offset,name) in offset bins
    Produce (in name bins):
      unique (offset,name) sample or nonsample records with unique names
      sample (offset,name,ranks[SampleSize]) merge records
      nonsample (offset,name,ranks[SampleSize]) for each nonsample offset

    Finish permuting each input bin. Then merge these different data sources,
    bin-by-bin, in order to create merge records. Each merge record will store
    (offset,name,ranks[SampleSize]), where the ranks[] are the next several
    ranks after offset that we have from recursion. Distribute these merge
    records into rank bins.

    Note, we could at this point include file numbers in for our final output
    in the merge records.

  sort_and_merge
    Given (in name bins):
      unique (offset,name) sample or nonsample records with unique names
      sample (offset,name,ranks[SampleSize]) merge records
      nonsample (offset,name,ranks[SampleSize]) for each nonsample offset
    Produce:
      (rank,offset)

    Given rank-bins (by name) in each of these categories:
     - first, finish sorting each individually. Note that this sort is always
       a radix sort (merge records comparing at a fixed period offset always
       use a particular rank in comparisons).
     - next, merge these sorted records in the bin to produce the final
       (rank,offset) records. This merge will use the general difference
       cover comparison function. Generally, we will be distributing the
       output into offset bins.

