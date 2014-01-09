#!/usr/bin/perl

use warnings;
use File::Temp qw/ tempfile tempdir /;
#use File::Path qw/ make_path remove_tree /;
use File::Path qw/ mkpath rmtree /;

#use FindBin qw($Bin);
use File::Basename;

srand(1);

# Use the current directory (build directory)
# to support VPATH build in make distcheck.
$Bin = ".";

$INDEX_TOOL = "$Bin/../dcx_cc/femto_index --no-limit-check --no-enable-core";
#$BWT_TOOL = "$Bin/../main/bwt_tool_qss";
#$CONSTRUCT_TOOL = "$Bin/../main/construct_tool --multifile";
$SEARCH_TOOL = "$Bin/../main_cc/femto_search";

#$MARK_PERIOD = 2;
#$CHUNK_SIZE = 4;
#$BLOCK_SIZE = 32;
#$BUCKET_SIZE = 8;
$MARK_PERIOD = 20;
$CHUNK_SIZE = 64;
$BLOCK_SIZE = 16*1024*1024;
$BUCKET_SIZE = 1024*1024;

# How many additional documents to include.
$NUM_ADDITIONAL_DOCS = 20;
# Size range for each additional document.
$MIN_ADD_DOC = 1;
$MAX_ADD_DOC = 500;
# How many random queries to include
$NUM_RANDOM_QUERIES = 50;
$MIN_RAND_QUERY = 1;
$MAX_RAND_QUERY = 16;

# if there are this many matches, stop using that query.
$NUM_DOCS_REGEXPD = 18;

$VERBOSE = 0;

@allbytes = ();
for( my $i=0; $i < 256; $i++ ) {
  push(@allbytes, chr($i));
}

$allbytes_str = join('', @allbytes);

@docs = ("a", "aa", "aab", "aac", "bb", "test", "fun", "\x00\x00",
         "\x00\x01\x00", "bannana", "seeresses", "equal", "un", "undo",
         "bbababcc",
         $allbytes_str, );


sub randrange {
  my $min = shift;
  my $max = shift;
  return $min + rand($max-$min);
}
sub randstr {
  my $sz = shift;
  my $type = shift; # 0--rand 1--any bytes 2--ascii 3--lowercase chars
  if( not defined($type) ) { $type = int(randrange(1,4)); }

  my @str_array = ();
  for( $j=0; $j < $sz; $j++ ) {
    my $ch = 0;
    if( $type == 1 ) {
      $ch = chr(int(randrange(0,256)));
    } elsif( $type == 2 ) {
      $ch = chr(int(randrange(40,126)));
    } elsif( $type == 3 ) {
      $ch = chr(int(randrange(97,123)));
    } else {
      die "Bad type $type in randstr";
    }

    # chars 40-126 are the nice ones. Generate those.
    push(@str_array, $ch);
  }
  return join('', @str_array);
}
sub x_escaped {
  my $str = shift;
  my @str_arr = split(//, $str);
  for my $ch (@str_arr) {
    if( $ch =~ /\W/ ) {
      # Escape it somehow.
      # If it's punctuation, we just backslash it.
      # except for single quote.. so we can '' for shell.
      if( $ch =~ /[[:punct:]]/ and $ch ne "'") {
        $ch = "\\$ch";
      } else {
        my $c = ord($ch);
        $ch = '\x' . (sprintf("%02x", $c));
      }
    } else {
      # OK
    }
  }
  my $ret = join('', @str_arr);
  #print "x_escaped '$str' is '$ret'\n";
  return $ret;
}
sub mysystem {
  my $cmd = shift;
  print "Running $cmd\n" if $VERBOSE;
  system($cmd) == 0 or die "Could not run $cmd: $!";
  return 0;
}
sub parseresults {
  my $fname = shift;
  $ret = [ ];
  open RESULTS, "< $dir/results" or die "Could not open $dir/results: $!";
  #print "Reading $dir/results\n";
  my ($path,$id,$dir,$suffix);
  while(<RESULTS>) {
    #print "Read $_\n";
    my $offsets = [];
    if( substr($_, 0, 1) eq "\t" ) {
      #print "Read offsets\n";
      my @parts = split(' ');
      for my $off (@parts) {
        #print "Read offset $off\n";
        push(@$offsets, int($off));
      }
      @$offsets = sort {$a <=> $b} @$offsets;
      pop(@$ret); # remove document-only.
    } else {
      chomp;
      ($id,$dir,$suffix) = fileparse($_);
      #print "Read fname $id\n";
    }
    push(@$ret, [int($id), $offsets]);
  }
  close RESULTS;
  # Sort the results.
  @$ret = sort { $$a[0] <=> $$b[0] } @$ret;

  return $ret;
}

sub print_results {
  my $prefix = shift;
  my $q = shift;
  my $offsets = shift;
  my $results = shift;
  #$expected{$q};
  my $quoted_q = $q; #x_escaped($q);
  my $perlq = $queries{$q}; #x_escaped($q);
  print "$prefix '$quoted_q' ie '$perlq' matches:\n";
  for my $r (@$results) {
    my $id = $$r[0];
    my $offs = $$r[1];
    print "doc $id:";
    if( $offsets ) {
      for my $off (@$offs) {
        print " " . $off;
      }
    }
    print "\n";
    #if( $VERBOSE > 2 ) {
    # print " -- " . x_escaped($docs[$doc]);
    #}
    #print "\n";
  }
}

sub checkresults {
  my $query = shift;
  my $expected_results = shift;
  my $got_results = shift;
  my $use_offsets = shift;
  my $fail = 0;

  # Now compare with the expected results.
  my $i;
  for( $i = 0; $i < @$expected_results and $i < @$got_results; $i++ ) {
    my $doc_exp = $$expected_results[$i][0];
    my $off_exp = $$expected_results[$i][1];
    my $doc_got = $$got_results[$i][0];
    my $off_got = $$got_results[$i][1];

    if( $doc_exp != $doc_got ) {
      print "Expected doc $doc_exp but got doc $doc_got\n";
      $fail = 1; 
    }
    if( $use_offsets ) {
      my $j;
      for( $j = 0; $j < @$off_exp and $j < @$off_got; $j++ ) {
        my $off_exp_j = $$off_exp[$j];
        my $off_got_j = $$off_got[$j];
        if( $off_exp_j != $off_got_j ) {
          print "In doc $doc_exp, expected offset $off_exp_j but got offset $off_got_j\n";
          $fail = 1; 
        } else {
          #print "In doc $doc_exp, matched $off_exp_j\n";
        }
      }
      for( ; $j < @$off_exp; $j++ ) {
        my $off_exp_j = $$off_exp[$j];
        print "In doc $doc_exp, expected but did not get offset $off_exp_j\n";
        $fail = 1; 
      }
      for( ; $j < @$off_got; $j++ ) {
        my $off_got_j = $$off_got[$j];
        print "In doc $doc_exp, got but did not expect offset $off_got_j\n";
        $fail = 1; 
      }
    }
  }
  for( ; $i < @$expected_results; $i++ ) {
    my $doc_exp = $$expected_results[$i][0];
    print "Missing document $doc_exp\n";
    $fail = 1;
  }
  for( ; $i < @$got_results; $i++ ) {
    my $doc_got = $$got_results[$i][0];
    print "Got extra document $doc_got\n";
    $fail = 1;
  }

  if( $fail ) {
    print_results("EXPECTED", $query, $use_offsets, $expected_results );
    print_results("GOT", $query, $use_offsets, $got_results );
  }
  return $fail;
}

# Add additional docs.
for( my $i=0; $i < $NUM_ADDITIONAL_DOCS; $i++ ) {
  # Generate a document.
  push(@docs, randstr(int(randrange($MIN_ADD_DOC, $MAX_ADD_DOC))));
}

# Print out the docs.
if( $VERBOSE > 2 ) {
  my $i = 0;
  for my $d (@docs) {
    print "doc $i -- " . x_escaped($d) . "\n";
    $i++;
  }
}

# Now do tests from docs.

%queries = ();

# Add tests for special characters.

$queries{"\t"} = 0;
$queries{"\n"} = 0;

# Add tests:
#  - first 2 characters of every doc.
#  - first 3 characters
#  - first 4 characters
#  - 2nd 2 characters
#  - 2nd 3 characters
#  - 2nd 4 characters
#  - random
for my $d (@docs) {
  $queries{substr($d,0,2)} = 0;
  $queries{substr($d,0,3)} = 0;
  $queries{substr($d,0,4)} = 0;
  $queries{substr($d,1,2)} = 0;
  $queries{substr($d,1,3)} = 0;
  $queries{substr($d,1,4)} = 0;
}


for(my $i = 0; $i < $NUM_RANDOM_QUERIES; $i++ ) {
  $rand_query = randstr(int(randrange($MIN_RAND_QUERY, $MAX_RAND_QUERY)));
  $queries{$rand_query} = 0;
}


# Make sure we don't have the empty query.
delete $queries{''};

sub perlre {
  my $s = shift;
  # \G -- anchor match on pos from last match (or setting)
  # /s -- . matches \n in addition to every other character
  return qr/\G(?:$s)/s;
}
{
  my %oldqueries = %queries;
  %queries = ( );
  # Compile all of the search strings
  for my $q (keys %oldqueries) {
    my $str = quotemeta($q);
    my $xstr = x_escaped($q);
    $queries{$xstr} = perlre($str);
  }
}

sub add_query {
  my $pat = shift;
  my $substrs = shift;
  my $perlre = "";
  my $femtore = "";
  for( my $i = 0; $i < length($pat); $i++ ) {
    my $ch = substr($pat, $i, 1);
    my $index = ord($ch) - ord("A");
    if( 0 <= $index and $index < @$substrs ) {
      $perlre .= $$substrs[$index][0];
      $femtore .= $$substrs[$index][1];
    } else {
      if( $ch eq "*" ) { $perlre .= "*?"; }
      elsif( $ch eq "+" ) { $perlre .= "+?"; }
      elsif( $ch eq "?" ) { $perlre .= "??"; }
      elsif( $ch eq "}" ) { $perlre .= "}?"; }
      else { $perlre .= $ch; }
      $femtore .= $ch;
    }
  }
  $queries{$femtore} = perlre($perlre);
}


# Add some fun regular expression queries.
for (my $index = 0; $index < $NUM_DOCS_REGEXPD and $index < @docs; $index++) {
  my $doc = $docs[$index];
  my $str;
  my ($a_ord, $b_ord);
  my @substrs;
  for( my $k = 0; $k < 7; $k++ ) {
    $str = chr(ord("a")+$k);
    push(@substrs, [$str, $str, ord($str)]);
  }
  for( my $k = 0; $k < length($doc) and $k < 7; $k++ ) {
    $str = substr($doc, $k, 1);
    $substrs[$k] = [quotemeta($str), x_escaped($str), ord($str)];
  }
  $a_ord = $substrs[0][2];
  $b_ord = $substrs[1][2];

  add_query("A.", \@substrs);
  add_query("B.D", \@substrs);
  add_query("AB.DE", \@substrs);
  add_query("A+B", \@substrs);
  add_query("(ABC)+D", \@substrs);
  add_query("A?B", \@substrs);
  add_query("AB?C", \@substrs);
  add_query("AB*C", \@substrs);
  add_query("A{2}D", \@substrs);
  add_query("A{2,}D", \@substrs);
  add_query("A{2,3}E", \@substrs);
  add_query("[AB]", \@substrs);
  add_query("[^AB]", \@substrs);
  if( $a_ord < $b_ord ) {
    add_query("[A-B]", \@substrs);
    add_query("[^A-B]", \@substrs);
  }
  add_query("AB|CD", \@substrs);
  add_query("(AB|CD)+E", \@substrs);
  add_query("AB(CD|EF)CD", \@substrs);
  add_query("AB(CD|EF)+CD", \@substrs);
}

if( $VERBOSE > 2 ) {
  for my $q (keys %queries) {
    print "query " . $q . " perlre " . $queries{$q} . "\n";
  }
}

{
  $numqueries = keys %queries;
  my $numd = @docs;
  print "Created $numd docs and $numqueries queries\n" if $VERBOSE > 0;
}

# Now form the expected results
# $expected{$query} = [[$id, [$offset0, $offset1, ...]], ... ]

print "Searching regexps with Perl\n" if $VERBOSE > 0;

$queryindex = 0;
%expected = ();
for my $q (keys %queries) {
  my $regexp = $queries{$q};
  # Look for q in all the documents!
  my $results = [];
  my $i = 0;
  print "Searching for $queryindex/$numqueries $regexp in docs\n" if $VERBOSE > 2;
  for my $d (@docs) {
    #print "Searching for $regexp in '$d'\n" if $VERBOSE > 2;
    my $offsets = [];
    my $lastend = -1;
    for( my $j = 0; $j < length($d); $j++ ) {
      pos($d) = $j;
      if( $d =~ /$regexp/gc ) {
        my $end = pos($d);
        if( $end == $lastend ) {
          # overwrite found match
          $$offsets[-1] = $j;
        } else {
          #print "Found match from $j to $end\n";
          push(@$offsets, $j);
          $lastend = $end;
        }
      }
    }
    if( @$offsets > 0 ) {
      push(@$results, [$i, $offsets]);
    }
    $i++;
  }
  $expected{$q} = $results;

  $queryindex++;
}

#Print out expected.
if( $VERBOSE > 2 ) {
  for my $q (keys %queries) {
    print_results("EXPECT", $q, 1, $expected{$q});
  }
}

# OK, Great! Now write out the documents to a directory.
#$dir = tempdir( CLEANUP => 1 );
$dir = "./testdir";
$tmp = "$dir/tmp";
$input = "$dir/input";
$output = "$dir/output";
rmtree($tmp);
rmtree($input);
rmtree($output);
mkpath($dir);
mkpath($tmp);
mkpath($input);
mkpath($output);

my $i=0;
for my $d (@docs) {
  my $docname = sprintf("%03d", $i);
  open DOC, "> $input/$docname" or die "Could not open $input/$docname: $!";
  print DOC $d;
  close DOC;
  $i++;
}

print "Creating index\n";

# Now run the BWT generation process.
#mysystem("$BWT_TOOL $tmp $MARK_PERIOD $input $output/out.bwt $output/out.info $CHUNK_SIZE $output/out.map");
#mysystem("$CONSTRUCT_TOOL $dir/index index $output/out.bwt $output/out.info $output/out.map block_size=$BLOCK_SIZE bucket_size=$BUCKET_SIZE");
mysystem("$INDEX_TOOL --tmp $tmp --param mark_period=$MARK_PERIOD --param chunk_size=$CHUNK_SIZE --param bucket_size=$BUCKET_SIZE --param block_size=$BLOCK_SIZE --outdir $dir/index $input");

print "Running queries\n";

# Now run all the queries.
for my $q (keys %queries) {
  my $expected_results = $expected{$q};
  my $quoted_q = $q; #x_escaped($q);
  print "Looking for $q\n";

  # Run looking for offsets.
  mysystem("$SEARCH_TOOL --offsets --output $dir/results $dir/index '$quoted_q'");
  my $got_results;
  my $fail;
  $got_results = parseresults("$dir/results");
  $fail = checkresults($q, $expected_results, $got_results, 1);
  die "FAIL --offsets" if $fail;
  mysystem("$SEARCH_TOOL --output $dir/results $dir/index '$quoted_q'");
  $got_results = parseresults("$dir/results");
  $fail = checkresults($q, $expected_results, $got_results, 0);
  die "FAIL" if $fail;

}

print "All queries PASS\n";

