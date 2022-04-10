#!/usr/bin/perl -w

# (c) 2006 Pavel Charvat <pchar@ucw.cz>
#
# Counts URLs for every n-th level domain (including subdomains)
#
# Typical usage:
#   bin/shep --sites -q 12345678 | count-domains.pl --level=2 --tree

use Getopt::Long;
use strict;

sub usage
{
  die "Usage: cat urls | count-domains.pl [<options>]
Options:
  --level=<num>            Subdomains grouping level (disabled=default=0, top-level=1)
  --hide-counts            Do not display counts
  --hide-total             Do not display total count
  --sort_by=<key>          Sort results by count|domain|none (default=count)
  --reverse                Reversed sorting order
  --filter-domains=<regex> Skip unmatched domains
  --filter-counts=<set>    Show only domains with such count (min|min-max|-max).
  --tree                   Show tree
";
}

my $hide_counts = 0;
my $hide_total = 0;
my $sort_by = "count";
my $max_level = 0;
my $reverse = 0;
my $filter_domains_regex = "";
my $filter_counts_set = "";
my $tree = 0;

GetOptions(
  "level=i" => \$max_level,
  "hide-counts!" => \$hide_counts,
  "hide-total!" => \$hide_total,
  "sort-by=s" => \$sort_by,
  "reverse!" => \$reverse,
  "tree!" => \$tree,
  "filter-domains=s" => \$filter_domains_regex,
  "filter-counts=s" => \$filter_counts_set,
) or usage();

my %hash;
my $total = 0;
if ($max_level <= 0) { $max_level = 10000; }

my $filter_counts_min = 0;
my $filter_counts_max = 1000000000;
if ($filter_counts_set =~ /([0-9]+)-([0-9])+/) { $filter_counts_min = $1; $filter_counts_max = $2; }
elsif ($filter_counts_set =~ /-([0-9]+)/) { $filter_counts_max = $1; }
elsif ($filter_counts_set =~ /([0-9]+)-?/) { $filter_counts_min = $1; }
elsif (!($filter_counts_set eq "")) { usage(); }
if ($filter_counts_min > $filter_counts_max) { usage(); }

while (<STDIN>) {
  if (/\bhttp:\/\/([^\/:\s]+)[\/:]/) {
    my $domain = $1;
    if ($filter_domains_regex eq "" || $domain =~ /$filter_domains_regex/) {
      $total++;
      my @parts = split(/\./, $domain);
      my $level = @parts;
      my $cut = ($level > $max_level) ? $level - $max_level : 0;
      if ($tree) {
        my $node = \%hash;
        for (my $i = $level - 1; $i >= $cut; $i--) {
          my $part = $parts[$i];
          $node = \%{$node->{$part}};
          $node->{'#'}++;
        }
      }
      else {
        if ($level > $max_level) {
          $domain = join(".", splice(@parts, $cut));
        }
        $hash{$domain}++;
      }
    }
  }
}

if (!$hide_total) { printf("%8d processed domains\n", $total); }
if ($tree) { show_tree(); }
else { show_list(); }

exit;

sub show_list
{
  my @keys = keys %hash;
  if ($sort_by eq "domain") {
    @keys = map(join(".", reverse split(/\./)), @keys);
    @keys = sort @keys;
    @keys = map(join(".", reverse split(/\./)), @keys);
  }
  elsif ($sort_by eq "count") {
    @keys = sort { $hash{$b} <=> $hash{$a} } @keys;
  }
  if ($reverse) {
    @keys = reverse @keys;
  }
  foreach my $key (@keys) {
    my $count = $hash{$key};
    if ($count >= $filter_counts_min && $count <= $filter_counts_max) {
      if (!$hide_counts) {
        printf("%8d ", $count);
      }
      print "$key\n";
    }
  }
}

sub show_tree
{
  if ($sort_by eq "domain") { $sort_by = 1; }
  elsif ($sort_by eq "count") { $sort_by = 2; }
  else { $sort_by = 0; }
  my $prefix = $hide_total ? "" : "    "; 
  show_branch(\%hash, "", $prefix);
}

sub show_branch
{
  my ($hash, $domain, $prefix) = (@_);
  my @keys = grep($_ !~ /^#/, keys %$hash);
  if ($domain) {
    my $count = $hash->{'#'};
    if ($count < $filter_counts_min || $count > $filter_counts_max) { return; }
    print $prefix;
    if (!$hide_counts) {
      printf("%8d ", $hash->{'#'});
    }
    print "$domain\n";
    $prefix = "$prefix    ";
    $domain = ".$domain";
  }
  if (scalar @keys > 1) {
    if ($sort_by == 1) {
      @keys = sort @keys;
    }
    elsif ($sort_by == 2) {
      @keys = sort { $hash->{$b}->{'#'} <=> $hash->{$a}->{'#'} } @keys;
    }
    if ($reverse) {
      @keys = reverse @keys;
    }
  }
  foreach my $key (@keys) {
    show_branch(\%{$hash->{$key}}, "$key$domain", $prefix);
  }
}

