#!/usr/bin/env python3

# This is a Python program to compute the checksums of fasta
# files in the same way as the FEMTO Chapel code does.
#
# The goal of it is to make it easy to verify that the Chapel code
# is doing I/O correctly.

import os
import sys
import hashlib

from Bio import SeqIO
from Bio import Seq

def hash_file(path):
    filehash = hashlib.sha256()
    recs = [ ]
    for rec in SeqIO.parse(path, "fasta"):
        recs.append(rec)

    # process the input
    # hash the forward sequences
    for rec in recs:
        filehash.update(b">")
        filehash.update(bytes(rec.seq.upper()))

    # hash the reverse complement sequences
    for rec in reversed(recs):
        filehash.update(b">")
        filehash.update(bytes(rec.seq.upper().reverse_complement()))

    print(filehash.hexdigest(), " " + path)

def hash_sequences(path):
    recs = [ ]
    for rec in SeqIO.parse(path, "fasta"):
        recs.append(rec)

    # process the input
    # hash the forward sequences
    for rec in recs:
        seqhash = hashlib.sha256()
        seqhash.update(b">")
        seqhash.update(bytes(rec.seq.upper()))
        print(seqhash.hexdigest(), " >" + rec.description)

    # hash the reverse complement sequences
    for rec in reversed(recs):
        seqhash = hashlib.sha256()
        seqhash.update(b">")
        seqhash.update(bytes(rec.seq.upper().reverse_complement()))
        print(seqhash.hexdigest(), " >" + rec.description + " [revcomp]")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: fasta-checksup.py <files-and-directories>")

    file_paths = [ ]

    for path in sys.argv[1:]:
        if os.path.isfile(path):
            file_paths.append(path)

        if os.path.isdir(path):
            for root, subdirs, files in os.walk(path):
                for filename in files:
                    file_paths.append(os.path.join(root, filename))

    file_paths.sort()

    for path in file_paths:
        hash_file(path)

    for path in file_paths:
        hash_sequences(path)
