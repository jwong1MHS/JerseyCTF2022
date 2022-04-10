#!/usr/bin/perl

# (c) 2007 Pavel Charvat <pchar@ucw.cz>

use strict;
use warnings;
use Getopt::Long;
use FileHandle;

use lib 'lib/perl5';
use Sherlock::Object;

sub usage
{
  die "Usage: vizualize-graph.pl [<options>] <buckets

Options:
  --start-url=<regex>     List of forest roots
  --hide-nofollow         Hide no-follow edges
  --hide-notarget         Hide edges with no target
  --hide-cross            Hide cross links
  --hide-reftexts         Hide reference texts
  --hide-unreachable      Hide unreachable URLs
  --show-text             Show text
  --show-content-type     Show content type
  --show-back-links       Show back references
";
}

my $start_url = "";
my $hide_nofollow;
my $hide_notarget;
my $hide_cross;
my $hide_reftexts;
my $hide_unreachable;
my $show_text;
my $show_content_type;
my $show_back_links;

GetOptions(
  "start-url=s" => \$start_url,
  "hide-nofollow" => \$hide_nofollow,
  "hide-notarget" => \$hide_notarget,
  "hide-cross" => \$hide_cross,
  "hide-reftexts" => \$hide_reftexts,
  "hide-unreachable" => \$hide_unreachable,
  "show-text" => \$show_text,
  "show-content-type" => \$show_content_type,
  "show-back-links" => \$show_back_links,
) or usage();
$start_url = "^$start_url\$";

my %hash = ();
my @heap = ();

sub heap_up($)
{
  my ($i) = (@_);
  while ($i) {
    my $j = int(($i + 1) / 2 - 1);
    last if $heap[$i]->{dist} >= $heap[$j]->{dist};
    my $x = $heap[$i]; $heap[$i] = $heap[$j]; $heap[$j] = $x;
    $heap[$i]->{heap} = $i; $heap[$j]->{heap} = $j;
    $i = $j;
  }
}

sub heap_down($)
{
  my ($i) = (@_);
  my $n = @heap;
  while (1) {
    my $j = ($i + 1) * 2 - 1;
    last if $j >= $n;
    $j = $j + 1 if $j < $n - 1 && $heap[$j]->{dist} > $heap[$j + 1]->{dist};
    last if $heap[$i]->{dist} <= $heap[$j]->{dist};
    my $x = $heap[$i]; $heap[$i] = $heap[$j]; $heap[$j] = $x;
    $heap[$i]->{heap} = $i; $heap[$j]->{heap} = $j;
    $i = $j;
  }
}

sub new_edge($$$)
{
  my ($src, $dest, $flags) = (@_);
  $hash{$dest} = { url => $dest } unless defined $hash{$dest};
  $hash{$src}->{out} = [] unless $hash{$src}->{out};
  $hash{$dest}->{in} = [] unless $hash{$dest}->{in};
  my $len = 1;
  $len /= 10 if $flags eq "Y";
  # FIXME
  my $e = { dest => $hash{$dest}, src => $hash{$src}, flags => $flags, len => $len };
  push @{$hash{$src}->{out}}, $e;
  push @{$hash{$dest}->{in}}, $e;
  return $e;
};

sub read_graph
{
  print "Reading graph...\n";

  my $fh = new FileHandle;
  $fh->open("<&=STDIN") or die "Could not open file\n";
  for (;;) {
    my $obj = new Sherlock::Object;
    my $res = $obj->read($fh);
    defined($res) or die("Input parse error");
    $res or last;

    my $url = $obj->get("U");

    $hash{$url} = { url => $url } unless $hash{$url};
    my $h = $hash{$url};

    next if $url =~ /^http:\/\/[^\/]*\/robots.txt$/;

    if (defined($h->{dist})) {
      print "Warning: Duplicate URL $url";
      next;
    }

    my %ref = (); my @otext = (); my @text = (); my $a;
    if (!$hide_reftexts || $show_text) {
      foreach $a ($obj->getarray("X")) { @text = (@text, 32, unpack("C*", $a)); }
      for (my $i = 0; $i < @text; ) {
        if ($text[$i] >= 0xe0) { for (my $n = 3; $n--; ) { push @otext, $text[$i++]; } }
        elsif ($text[$i] >= 0xc0) { for (my $n = 2; $n--; ) { push @otext, $text[$i++]; } }
        elsif ($text[$i] >= 0xa0 && $text[$i] < 0xb0) {
	  my @reftext = ();
	  my $no = $text[$i + 1] - 0x80 + ($text[$i] - 0xa0) * 64;
	  for ($i += 2; $text[$i] != 0xb0; ) {
	    if ($text[$i] >= 0xe0) { for (my $n = 3; $n--; ) { push @reftext, $text[$i++]; } }
	    elsif ($text[$i] >= 0xc0) { for (my $n = 2; $n--; ) { push @reftext, $text[$i++]; } }
	    elsif ($text[$i] >= 0x80) { $i++; push @reftext, 0x20; }
	    else { push @reftext, $text[$i++]; }
	  }
	  $ref{$no} = pack("C*", @reftext);
	  push(@otext, @reftext);
	  $i++;
        }
	elsif ($text[$i] >= 0x80) { $i++; push @otext, 0x20; }
	else { push @otext, $text[$i++]; };
      }
      if ($show_text) {
	my $t = pack("C*", @otext);
	$t =~ s/^\s+//;
	$t =~ s/\s+$//;
	$t =~ s/\s\s+/ /g;
        $h->{text} = $t;
      }
    }

    foreach my $attr ("R", "I") {
      foreach $a ($obj->getarray($attr)) {
        if (($a =~ /([^ ]*) ([^ ]*)(.*)$/) && (!$hide_nofollow || !$3)) {
	  my $e = new_edge($url, $1, ($3 ? ($attr eq "R" ? "r" : "i") : $attr));
	  if ($ref{$2} && $ref{$2} =~ /^\s*(.*[^\s]+)\s*$/) {
	    $e->{text} = $1;
	  }
        }
      }
    }
    foreach $a ($obj->getarray("Y")) {
      if (($a =~ /([^ ]*) /)) {
        new_edge($url, $1, "Y");
      }
    }

    $a = $obj->get("T");
    if ($a) { $h->{note} = $a; }

    if ($url =~ m/$start_url/) {
      $h->{dist} = 0;
      $h->{init} = 1;
    }
    else {
      $h->{dist} = 1000000000;
    }

    $h->{heap} = @heap;
    $heap[@heap] = $h;
  }
  $fh->close();
}

sub weights
{
  print "Computing weights...\n";
  my $n = @heap;
  my $j = int(rand($n));
  my @w = ();;
  for (my $i = 0; $i < $n; $i++) { $w[$i] = 0; }
  for (my $i = $n * 50; $i--; ) {
    my $c = rand(1);
    if ($c < 0.1) { $j = int(rand(@heap)); }
    elsif ($heap[$j]->{out}) {
      $c = int(rand(1 + @{$heap[$j]->{out}}));
      if ($c < @{$heap[$j]->{out}}) { $j = $c; }
    }
    $w[$j]++;
  }
  for (my $i = 0; $i < $n; $i++) { $heap[$i]->{dist} += ($n * 50 - $w[$i]) / ($n + 1); heap_up($i); }
}

sub dijkstra
{
  print "Building tree...\n";
  for (my $i = 0; $i < @heap; $i++) {
    $heap[$i]->{dist} = $heap[$i]->{dist} - 100 if defined $heap[$i]->{dist} && !$heap[$i]->{in};
  }
  while (@heap) {
    my $h = $heap[0];
    my $url = $h->{url};

    my $x = pop @heap;
    if (@heap) { $heap[0] = $x; heap_down(0); }

    if ($h->{dist} >= 999999000) { if ($hide_unreachable) { next; } else { $h->{dist} = 0; } }
    my $dist = $h->{dist};
    $h->{done} = 1;

    foreach my $e (@{$h->{out}}) {
      my $dest = $e->{dest};
      if (defined($dest->{dist}) && !$dest->{done}) {
        my $w = $dist + $e->{len};
	if ($w < $dest->{dist}) {
          $dest->{dist} = $w;
	  $dest->{from} = $h;
	  heap_up($dest->{heap});
	}
      }
    }
  }
}

sub display_tree
{
  my ($from, $h, $depth, $eflags, $etext) = (@_);
  for (my $i=0; $i<$depth; $i++) { print "    "; }
  printf "%4s", $eflags;
  print "--> $h->{url}";
  print " [$etext]" if $etext;
  print " ($h->{note})" if defined $h->{note};
  print "\n";
  return if $eflags =~ /[\.\*]/;
  if ($h->{text}) {
    for (my $i=0; $i<$depth; $i++) { print "    "; }
    print "        X $h->{text}\n";
  }
  if ($show_back_links && $h->{in}) {
    foreach $b (@{$h->{in}}) {
      if (!($b->{src}->{url} eq $from)) {
        for (my $i=0; $i<$depth; $i++) { print "    "; }
	print "        <-- $b->{src}->{url}";
        print " [$b->{text}]" if $b->{text};
	print "\n";
      }
    }
  }
  my $edges = $h->{out};
  foreach my $e (@$edges) {
    my $f;
    if ($e->{dest}->{from} && $e->{dest}->{from} == $h) { $f = "" }
    elsif (defined($e->{dest}->{dist})) { $f = "\*"; next if $hide_cross; }
    else { $f = "\."; next if $hide_notarget; }
    display_tree($h->{url}, $e->{dest}, $depth + 1, $f . $e->{flags}, $e->{text});
  }
}

sub display
{
  print "Results:\n";
  foreach my $h (values(%hash)) {
    if (defined($h->{dist}) && !$h->{from} && (!$hide_unreachable || $h->{init})) {
      display_tree("", $h, 0, $h->{init} ? "S" : "s", "");
    }
  }
}

read_graph();
weights();
dijkstra();
display();
