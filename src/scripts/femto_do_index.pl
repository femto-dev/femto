#!/usr/bin/env perl

use warnings;
use File::Basename;
use File::Copy;

$DIR=undef;

# uses femto_size or femto_size_ext for extensions other than .list .list0
$INDEXARGS="";
$INDEXTMP=undef;
$INDEXCMD = "femto_index";
$SIZECMD = "femto_size";

$USE_INDEXCMD = undef;
$USE_SIZECMD = undef;

$GB=1024*1024*1024;
$MINSZ=1*$GB; # minimum index size 1GiB
$MAXSZ=128*$GB; # maximum index size 128GiB
$VERBOSE=0;

sub usage {
  print "Usage: index.pl --dir <data-set-directory> --index-tmp <index-tmp>\n";
  print "Optional arguments\n";
  print "  --size-cmd <index-command>\n";
  print "  --index-cmd <index-command>\n";
  print "      index-command will have e.g. _ext appended for .ext files\n";
  print "  --index-args <index-command-args>\n";
  print "  --verbose\n";
  print "  --min-index-size-gb <number-in-gibibytes>\n";
  print "  --max-index-size-gb <number-in-gibibytes>\n";
  print "  --min-index-size-bytes <number-in-bytes>\n";
  print "  --max-index-size-bytes <number-in-bytes>\n";
  exit 1;
}

while (@ARGV > 0) {
  $_ = shift @ARGV;
  if ( $_ eq "--dir" ) {
    $DIR = shift @ARGV;
  } elsif ( $_ eq "--size-cmd" ) {
    $SIZECMD = shift @ARGV;
  } elsif ( $_ eq "--index-cmd" ) {
    $INDEXCMD = shift @ARGV;
  } elsif ( $_ eq "--index-args" ) {
    $INDEXARGS .= shift @ARGV;
  } elsif ( $_ eq "--index-tmp" ) {
    $INDEXTMP = shift @ARGV;
  } elsif ( $_ eq "--verbose" ) {
    $VERBOSE++;
  } elsif ( $_ eq "--min-index-size-gb" ) {
    $MINSZ = $GB * (shift @ARGV);
  } elsif ( $_ eq "--max-index-size-gb" ) {
    $MAXSZ = $GB * (shift @ARGV);
  } elsif ( $_ eq "--min-index-size-bytes" ) {
    $MINSZ = (shift @ARGV);
  } elsif ( $_ eq "--max-index-size-bytes" ) {
    $MAXSZ = (shift @ARGV);
  } else {
    print "Unkown argument $_\n";
    usage();
  } 
}


if (not defined $DIR) {
  usage();
}
if (not defined $INDEXTMP) {
  usage();
}


if (not -d $DIR) {
  print "Directory $DIR does not exist\n";
  exit 1;
}

if (not -d "$DIR/staging") {
  print "Directory $DIR/staging does not exist\n";
  exit 1;
}

# Make sure that the other directories exist.
if (not -d "$DIR/working") {
  mkdir "$DIR/working";
}
if (not -d "$DIR/indexed") {
  mkdir "$DIR/indexed";
}
if (not -d "$DIR/indexes") {
  mkdir "$DIR/indexes";
}
if (not -d "$DIR/logs") {
  mkdir "$DIR/logs";
}

$TMPIN = "$INDEXTMP/tmpin";
# Create temporary storage for a copy of input files.
if (not -d $TMPIN ) {
  mkdir $TMPIN;
}

# Take care of ulimit business. Large index
# jobs need lots of files in the distribution
# phase; Linux supports millions of files,
# so we just check that we have at least 10,000
# available.
{
  my $filelimit = `sh -c "ulimit -n"`;
  if( $filelimit < 10000 ) {
    # Check that we can increase ulimit.
    my $got = `sh -c "ulimit -n 10000"`;
    $? == 0 or die "Could not increase ulimit to 10000";
  }
}

# Go through all of the files in the index directory.
do {
  $GOTWORK = 0;
  $SZ = 0;
  $ONLYSUFFIX = undef;
  @myfiles = ( );

  opendir DH, "$DIR/staging";
  for my $file (readdir DH) {
    next if $file eq ".";
    next if $file eq "..";
    my $staging = "$DIR/staging/$file";
    my $working = "$DIR/working/$file";
    my ($name, $path, $suffix) = fileparse($file, qr/\.[^.]*/);
    if( not defined $ONLYSUFFIX ) {
      $ONLYSUFFIX = $suffix;
      $USE_INDEXCMD = $INDEXCMD;
      $USE_SIZECMD = $SIZECMD;
      if( $ONLYSUFFIX eq "" or
          $ONLYSUFFIX eq ".list" or
          $ONLYSUFFIX eq ".list0" ) {
        # OK! these are supported by the normal index_cmd.
      } else {
        my $usesuffix = $ONLYSUFFIX;
        $usesuffix =~ s/\./_/g;
        $USE_INDEXCMD .= $usesuffix;
        $USE_SIZECMD .= $usesuffix;
      }
    }
    next if $ONLYSUFFIX ne $suffix;
    # hard-link from staging/uuid to working/uuid
    if( link $staging, $working ) {
      # Size this file.
      my $got = `$USE_SIZECMD $working`;
      $? == 0 or die "Could not $USE_SIZECMD $working: $!";
      my ($bytes, $rest) = split(' ', $got, 2);
      die "File $DIR/working/$file has zero bytes" if $bytes == 0;
      $SZ += $bytes;
      push(@myfiles, {FILE=>$file, BYTES=>$bytes, WORKING=>"$DIR/working/$file", STAGING=>"$DIR/staging/$file", INDEXED=>"$DIR/indexed/$file", NAME=>$name, SUFFIX=>$suffix, TMPCOPY=>"$TMPIN/$file"});
      print "Got staging/$file of size $bytes\n" if $VERBOSE > 0;
    } else {
      print "Link already exists for $staging $working\n" if $VERBOSE > 0;
    }
    last if $SZ >= $MAXSZ;
  }
  closedir DH;
  
  # Sort files from largest to smallest.
  @myfiles = sort { $$b{BYTES} <=> $$a{BYTES} } @myfiles;

  # Now we have as many files as there were, up to a maximum 
  # size, owned by us, and linked into working.
  # We need to unlink in working the last ones if we're above our maximum,
  # and we need to unlink all of them if we're below our minimum.
  
  while( ($SZ > $MAXSZ or $SZ < $MINSZ) and (@myfiles > 0) ) {
    my $info = pop(@myfiles);
    my $file = $$info{FILE};
    my $bytes = $$info{BYTES};
    $SZ -= $bytes;
    print "Unlinking working/$file of size $bytes\n" if $VERBOSE > 0;
    unlink($$info{WORKING}); # remove from working - note it remains in staging
  }

  if( $SZ > 0 and (@myfiles > 0) ) {
    # Accumulate index tool arguments

    $INDEXNAME = undef;
    $PATHARGS = "";
    $COUNT = 0;
    for my $info (@myfiles) {
      my $file = $$info{FILE};
      my $working = $$info{WORKING};
      my $tmpcopy = $$info{TMPCOPY};
      my $bytes = $$info{BYTES};
      my $name = $$info{NAME};
      my $suffix = $$info{SUFFIX};
      my $filetype = "";
      $filetype = "--from-list" if $suffix eq ".list";
      $filetype = "--from-list0" if $suffix eq ".list0";
      $PATHARGS .= " $filetype $tmpcopy";
      $INDEXNAME = $name;
      $COUNT++;
      print "Will index working/$file of size $bytes\n" if $VERBOSE > 0;
      print "Copying $working to $tmpcopy\n" if $VERBOSE > 0;
      copy($working, $tmpcopy);
    }

    die "no name!" if not defined $INDEXNAME;

    $INDEXNAME .= "..$COUNT" . ".femto";

    $index = "$DIR/indexes/$INDEXNAME";
    $log = "$DIR/logs/$INDEXNAME" . ".log";

    my $INDEX_RUN = "$USE_INDEXCMD --tmp $INDEXTMP $INDEXARGS --outfile $index $PATHARGS >> $log 2>&1";

    open(LOG, ">", $log) or die "Can't create $log: $!";
    for my $info (@myfiles) {
      print LOG $$info{FILE};
      for my $k (keys %$info) {
        print LOG ' ' . $k . " => " . $$info{$k};
      }
      print LOG "\n";
    }
    print LOG "Total size is $SZ\n";
    print LOG $INDEX_RUN . "\n";
    close(LOG);

    print "Running $INDEX_RUN\n" if $VERBOSE > 0;
    system($INDEX_RUN) == 0 or die "Could not run $INDEX_RUN: $!";

    # link from working/ to indexed/
    # unlink staging/ and working/ files.
    for my $info (@myfiles) {
      my $file = $$info{FILE};
      my $working = $$info{WORKING};
      my $staging = $$info{STAGING};
      my $indexed = $$info{INDEXED};
      my $tmpcopy = $$info{TMPCOPY};
      
      # link from working/uuid to indexed/uuid
      link $working, $indexed or die "Could not link $working $indexed";
      # unlink tmp copy
      unlink $tmpcopy;
      # unlink staging/uuid
      unlink $staging;
      # unlink working/uuid
      unlink $working;
    }
    $GOTWORK = 1;
  }
} while($GOTWORK);

print "No more work; exiting\n";
exit 0;

