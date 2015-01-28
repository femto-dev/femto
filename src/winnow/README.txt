WINNOWING AND HASH CLUSTERING               --------------------

In this section, we will discuss an implementation of the Winnowing algorithm
first described by Schleimer, Wilkerson, Aiken in "Winnowing: Local Algorithms
for Document Fingerprinting" and also an extension to it for clustering groups
of winnow hashes.

The paper "Winnowing: Local Algorithms for Document Fingerprinting" is
available at:
 http://theory.stanford.edu/~aiken/publications/papers/sigmod03.pdf

The Winnowing algorithm is useful for producing hashes that can be used to
find common sequences while offering some cut-down in the data volume. The
original paper described using this technique for plagarism detection.

In the winnowing algorithm (as described in the paper above), a rolling hash
is computed for byte sequences of some specific length K.  Then, these hashes
are taken in rolling windows of size T. The minimum hash value of each window
is output - but a particular minimum hash value is only output once even if it
is the minimum for many windows.

This software also includes an implementation of an algorithm to find the
longest common patterns of the same winnow hashes. This is useful to find long
regions of multiple documents that are very similar, as might be useful in
alignment, plagarism detection, or version tracking.

The grouping algorithm works in passes over the set of winnow hashes as
follows. It operates on entries containing
 (position,hash,number of errors,number of combined hashes,score,length)
which are initially populated with (position,hash,0,1,0,K) from the original
winnowing algorithm. 


Then, the algorithm constructs an array of candidates that combine hash and
the next hash in the original data (ie, 2-tuples of hashes sorted by
position). Each candidate is initialized with
 (position,hash,next_hash,length,0,number of errors,0,number of combined
and also candidates that skip a single hash are created and initialized with
 (position,hash,next_next_hash,length,0,number of errors,0,
  number of combined hashes, 0, 1)

Then, these candidates are sorted by hash/next_hash. Within a given hash
value, candidates for a given next_hash are combined if there there are enough
of them that they account for at least 2/3 of the candidates with that hash
value (and any next_hash value).

When the hashes are combined, a new candidate hash is produced that would be
the same as if the hashed strings were concatenated and then hashed. In this
way, the amount of information (the number of bytes in each hash) stays the
same even as each hash can account for a longer and longer sequence in the
original data. The number of errors in each hash/next hash is added. Hash
pairs that skipped a position have their number of errors increased by one.

Then, new hashes are created from the candidate hashes; they are sorted by
position, and hashes that are subsumed by another hash or that are duplicates
but with higher number of errors are removed.

The process is repeated in order to find an approximate set of long and common
sequences to a set of data.

See the source code files
 cluster_hashes.c  dump_hashes.c  hash.c  hash.h

