#!/usr/bin/env perl

use warnings;
use File::Basename;

$DIR=undef;
$PATTERN=undef;
$SCANCMD="femto_scan";
$SEARCHCMD="femto_search";
$VERBOSE=0;
$TOOLARGS="";

$COUNT=0;

sub usage {
  print "Usage: search.pl --dir <data-set-directory> --pattern <pattern>\n";
  print "Optional arguments:\n";
  print " --scan-cmd <scan-command>\n";
  print "      scan-command will have e.g. _ext appended for .ext files\n";
  print " --search-cmd <index-search-command>\n";
  print " --verbose\n";
  print " --max_results <max-number-of-results>  set the maximum number of results\n";
  print " --offsets    request document offsets\n";
  print " --count    ask only for the number of results\n";
  print " --matches    show strings matching search\n";
  print " --null    use zero-bytes instead of newlines between results\n";
  print " --icase    do case-insensitive searching\n";
  print " --output <filename>   output query results to a file instead of stdout\n";
  exit 1;
}

while (@ARGV > 0) {
  $_ = shift @ARGV;
  if ( $_ eq "--dir" ) {
    $DIR = shift @ARGV;
  } elsif ( $_ eq "--pattern" ) {
    $PATTERN = shift @ARGV;
  } elsif ( $_ eq "--scan-cmd" ) {
    $SCANCMD = shift @ARGV;
  } elsif ( $_ eq "--search-cmd" ) {
    $SEARCHCMD = shift @ARGV;
  } elsif ( $_ eq "--verbose" ) {
    $VERBOSE++;
  } elsif ( $_ eq "--max_results" ) {
    $TOOLARGS .= " --max_results " . (shift @ARGV);
  } elsif ( $_ eq "--offsets" ) {
    $TOOLARGS .= " --offsets";
  } elsif ( $_ eq "--count" ) {
    $TOOLARGS .= " --count";
    $COUNT = 1;
  } elsif ( $_ eq "--matches" ) {
    $TOOLARGS .= " --matches";
  } elsif ( $_ eq "--null" ) {
    $TOOLARGS .= " --null";
  } elsif ( $_ eq "--icase" ) {
    $TOOLARGS .= " --icase";
  } elsif ( $_ eq "--output" ) {
    $TOOLARGS .= " --output " . (shift @ARGV);
  } else {
    usage();
  } 
}


if (not defined $DIR) {
  usage();
}
if (not defined $PATTERN) {
  usage();
}

if ( not -d $DIR ) {
  print "$DIR does not exist\n";
  exit 1;
}

if ( not (-d "$DIR/staging" or -d "$DIR/indexes") ) {
  print "$DIR is not a data-set with staging and indexes subdirs\n";
  exit 1;
}

if( -d "$DIR/staging" ) {
  # We have data in staging. Run scan_tool on each file in it.
  opendir DH, "$DIR/staging";
  for my $file (readdir DH) {
    next if $file eq ".";
    next if $file eq "..";
    my ($name, $path, $ONLYSUFFIX) = fileparse($file, qr/\.[^.]*/);

    $USE_SCANCMD = $SCANCMD;
    
    if( $ONLYSUFFIX eq "" or
        $ONLYSUFFIX eq ".list" or
        $ONLYSUFFIX eq ".list0" ) {
      # OK handled by normal tool
    } else {
      my $usesuffix = $ONLYSUFFIX;
      $usesuffix =~ s/\./_/g;
      $USE_SCANCMD .= $usesuffix;
    }
    #$uid = $name;
    $staging = "$DIR/staging/$file";

    my $FILETYPE = "";
    $FILETYPE = "--from-list" if $ONLYSUFFIX eq ".list";
    $FILETYPE = "--from-list0" if $ONLYSUFFIX eq ".list0";

    $SCAN_RUN = "$USE_SCANCMD $TOOLARGS $FILETYPE $staging \"$PATTERN\"";
    print "Running $SCAN_RUN\n" if $VERBOSE > 0;
    if( $COUNT ) {
      my $got = `$SCAN_RUN`;
      $? == 0 or die "Could not scan $SCAN_RUN: $!";
      push(@COUNTS, $got);
    } else {
      system("$SCAN_RUN") == 0 or die "Could not scan $SCAN_RUN: $!";
    }
  }
  closedir DH;
}
 
if( -d "$DIR/indexes" ) {
  # We have data in indexes. Run search_tool on each index.
  my $indexes = "";
  opendir DH, "$DIR/indexes";
  for my $uid (readdir DH) {
    next if $uid eq ".";
    next if $uid eq "..";
    $indexes .= " $DIR/indexes/$uid";
  }
  closedir DH;

  $SEARCH_RUN = "$SEARCHCMD $TOOLARGS $indexes \"$PATTERN\"";
  print "Running $SEARCH_RUN\n" if $VERBOSE > 0;
  if( $COUNT ) {
    my $got = `$SEARCH_RUN`;
    $? == 0 or die "Could not search $SEARCH_RUN: $!";
    push(@COUNTS, $got);
  } else {
    system("$SEARCH_RUN") == 0 or die "Could not search $SEARCH_RUN: $!";
  }
}

if( $COUNT ) {
  $TOT=0;
  for my $got (@COUNTS) {
    my ($bytes, $rest) = split(' ', $got, 2);
    $TOT += $bytes;
  }
  printf("% 4d total matches\n", $TOT);
}

