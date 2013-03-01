This contains handy wrapper scripts for the FEMTO
indexer, scan tool (for unindexed data), and index search tool.

Each takes a --dir option which needs a "data set directory" as an argument.
The search tools return particular formats; for a document search:
document-name
document-name
document-name

for a document+offsets search:
document-name
<tab>offset offset offset
document-name
<tab>offset offset offset
document-name
<tab>offset offset offset


If --null is given, the newlines are replaced by \0 bytes so that the output is unambiguous, even in the presence of document names containing newlines.

Each data set directory contains subdirectories and uuid-named files:
staging/uuid#
indexed/uuid#
working/uuid#
indexes/uuid#

uuids can be generated with the uuidgen command.

These files are 'work lists' generally.

-- Data producer:
  Producer of data produces data somewhere (e.g. ./input or elsewhere) and
  when it's done with a directory/set it hardlinks its output file to
  staging/uuid#

-- Indexer consumer:
  index.pl will go through the unindexed data in staging
      -- hard-link from staging/uuid to working/uuid
         if we get EEXIST, somebody else has it; move on to the next
      -- index working/uuid, saving output to index/uuid/ (which may be a symbolic link)
      -- hard-link from working/uuid into indexed/uuid
      -- unlink 1st staging/uuid and 2nd working/uuid

-- Search system:
  search.pl will scan anything in staging/ first and then search anything in indexed/

