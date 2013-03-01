#!/usr/bin/env perl

use warnings;

$DIR=undef;

sub usage {
  print "Usage: restart.pl --dir <data-set-directory>\n";
  exit 1;
}

while (@ARGV > 0) {
  $_ = shift @ARGV;
  if ( $_ eq "--dir" ) {
    $DIR = shift @ARGV;
  } else {
    usage();
  } 
}


if (not defined $DIR) {
  usage();
}

if ( not -d $DIR ) {
  print "$DIR does not exist\n";
  exit 1;
}

if ( not -d "$DIR/working" ) {
  print "$DIR/working does not exist\n";
  exit 0;
}

print "Clearing files from $DIR/working\n";

# Remove anything from $DIR/working
opendir DH, "$DIR/working";
for my $uid (readdir DH) {
  next if $uid eq ".";
  next if $uid eq "..";
  $working = "$DIR/working/$uid";
  unlink $working;
}

