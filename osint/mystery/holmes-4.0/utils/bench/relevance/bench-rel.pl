#!/usr/bin/perl
#	Relevance benchmark of Sherlock search server
#	(c) 2003, Robert Spalek <robert@ucw.cz>
#	(c) 2007, Martin Mares <mj@ucw.cz>

use Getopt::Long;
use strict;
use warnings;

use lib 'lib/perl5';
use Sherlock::Query();
use Data::Dumper;

sub usage
{
	die "Usage: cat tests | bin/bench-rel [options]
Options:
  --server=<server>:<port>   Ask custom search server or mux (default: localhost:8192)
  --prefix=<str>             Custom query prefix
  --debug                    Show debug messages
";
}

my $search_svr = 'localhost:8192';
my $query_prefix = 'show 1..20 context 0 urls 256 morph 2 spell 2 syn 0 accents 0 sitemax 1';
my $debug_level = 0;

GetOptions(
	"server=s" => \$search_svr,
	"prefix=s" => \$query_prefix,
	"debug!" => \$debug_level,
) or usage();

sub debug($$$);
sub pos2weight($);
sub normalize_url($);
sub format_url($$$);

# Parse the input into one array
$/ = "";
my @tests = <>;
$/ = "\n";

# Parse each record into QUERY and hash of URL -> BONUS
my (%expurls, %fndurls);
foreach (@tests)
{
	my ($query, $urls) = /^([^\n]*)\n(.*)\n$/s;
	my @urls = map {
		my ($weight, $url) = split /\s/;
		(normalize_url($url), $weight);
	} split /\n/, $urls;
	$expurls{$query} = { @urls };
	$_ = $query;
}

# Ask the search server every QUERY and parse the responses
foreach my $query (@tests)
{
	my $q = new Sherlock::Query($search_svr);
	debug($q, $query, "Query");
	my $res = $q->query("$query_prefix $query");
	debug($q, $res, "Response");
	my @queryurls;
	if ($res =~ /^\+/)
	{
		debug($q, $q->{HEADER}, "Header");
		my $i = 0;
		foreach my $c (@{$q->{CARDS}})
		{
			$i++;
			my $urlrecU = [ $i, $c->get("Q"), 'U' ];
			my $urlrecy = [ $i, $c->get("Q"), 'y' ];
			my $urlrecb = [ $i, $c->get("Q"), 'b' ];
			foreach my $url ($c->getarray("(U"))
			{
				# concatenate the URL's of the card to the big list
				my @cardurls = map { (normalize_url($_), $urlrecy); } ($url->getarray("y"));
				@queryurls = (@queryurls, normalize_url($url->get('U')), $urlrecU, @cardurls);
				@queryurls = (@queryurls, normalize_url($url->get('b')), $urlrecb) if $url->get('b');
			}
			debug($q, $c, "Card $i");
		}
		debug($q, $q->{FOOTER}, "Footer");
	}
	$fndurls{$query} = { @queryurls };
	debug($q, $fndurls{$query}, "Found URL's");
	debug($q, $expurls{$query}, "Expected URL's");
}

# Compare these two datasets
my %grades;
my $total_grade = 0;
foreach my $query (@tests)
{
	print "Query:\t\t$query\n";
	my $fnd = $fndurls{$query};
	my $exp = $expurls{$query};
	my @fnd = sort { $fnd->{$a}->[0] <=> $fnd->{$b}->[0] } keys %$fnd;
	my @exp = sort { $exp->{$b} <=> $exp->{$a} } keys %$exp;
	my $grade = 0;
	my $max_grade = 0;
	my $i = 1;
	my $url;
	foreach $url (@exp)
	{
		$grade += $exp->{$url} * pos2weight($fnd->{$url});
		$max_grade += $exp->{$url} * pos2weight([$i++]) if $exp->{$url} > 0;
	}
	my $rel_grade = $grade / $max_grade;
	print "Total grade:\t$rel_grade = $grade / $max_grade\n";
	print "Found URL's:\n";
	foreach $url (@fnd)
	{
		print format_url($fnd, $exp, $url);
	}
	print "Expected URL's that have not been found:\n";
	foreach $url (@exp)
	{
		print format_url($fnd, $exp, $url) if !$fnd->{$url};
	}
	print "\n";
	$grades{$query} = $rel_grade;
	$total_grade += $rel_grade;
}

# Print total results
foreach my $query (@tests)
{
	printf("%8.5f : %s\n", $grades{$query}, $query);
}
printf("%8.5f = Total\n", $total_grade);
printf("%8.5f = Average (%d tests)\n", $total_grade / @tests, 0 + @tests);

### Subroutines:

# Verbose debug dumps
sub debug($$$)
{
	my ($q, $what, $title) = @_;
	if ($debug_level) {
		print "$title: \n" if $title;
		if (ref $what eq "") {
			print "\t$what\n";
		} elsif (ref $what eq "Sherlock::Object") {
			$what->write_indented(\*STDOUT, "\t");
		} else {
			print Dumper($what);
		}
	}
}

# Computing the weight of a given position at the output
sub pos2weight($)
{
	my ($urlrec) = @_;
	return 0 if !$urlrec;
	my $pos = $urlrec->[0];
	return 105 - 5*$pos;
}

# Cut www. prefix
sub normalize_url($)
{
	my ($url) = @_;
	$url =~ s|//www\.|//|;
	return $url;
}

# Formats the output line about one URL
sub format_url($$$)
{
	my ($fnd, $exp, $url) = @_;
	my $wt = $exp->{$url};
	$wt = 0 if !$wt;
	my $urlrec = $fnd->{$url};
	my $poswt = pos2weight($urlrec);
	$urlrec = [ 0, 0, '0' ] if !$urlrec;
	return "$wt\t$poswt\t$urlrec->[2] #$urlrec->[0] Q$urlrec->[1]\t$url\n";
}
