#!/usr/bin/perl
#
#	Sherlock Query Sender
#	(c) 2001--2007 Martin Mares <mj@ucw.cz>
#

use strict;
use warnings;
use Getopt::Long;

use lib 'lib/perl5';
use Sherlock::Query;

sub do_query($);

my $host = "localhost";
my $port = 8192;
my $requests = 1;
my $headers = 1;
my $body = 1;
my $footer = 1;
my $stats = 0;
my $control = 0;
my $debug = 0;
my $parsed = 0;
my $indented = 0;

Getopt::Long::Configure("bundling");
GetOptions(
	"host|h=s" => \$host,
	"port|p=i" => \$port,
	"silent|s" => sub { $headers=$body=$footer=$stats=$requests=0; },
	"requests!" => \$requests,
	"header!" => \$headers,
	"body!" => \$body,
	"footer!" => \$footer,
	"stats!" => \$stats,
	"control!" => \$control,
	"debug!" => \$debug,
	"parsed!" => \$parsed,
	"indented!" => \$indented,
	# Need to repeat that since getopt doesn't support "long|short!" spec
	"R" => \$requests,
	"H" => \$headers,
	"B" => \$body,
	"F" => \$footer,
	"P" => \$parsed,
	"S" => \$stats,
	"C" => \$control,
	"d" => \$debug,
	"i" => \$indented,
) || die "Syntax: query [<options>] [<query>]

Options:
-h, --host=hostname	Select search server host
-p, --port=port		Select search server port
-s, --silent		--norequests --noheaders --nobody --nofooter --nostats
-R, --requests		Show requests (default: on)
-H, --header		Show reply headers (default: on)
-B, --body		Show reply body (default: on)
-F, --footer		Show reply footer (default: on)
-S, --stats		Show statistics
-C, --control		Send a control command instead of query
-d, --debug		Show a debugging dump
-P, --parsed		Show the reply as a parsed object instead of raw lines
-i, --indented		Show sorted attributes and indented subobjects (implies -P)
";

my $total_ok = 0;
my $total_err = 0;
my $total_time = 0;
my $total_cached = 0;
my %totals = ();
if (@ARGV) {
	my $query = join(' ', @ARGV);
	do_query($query);
} else {
	while (<>) {
		chomp;
		do_query($_);
	}
}
if ($stats) {
	print "## Total queries: $total_ok OK + $total_err FAILED\n";
	print "## Total time: $total_time\n";
	print "## Cache hits: $total_cached\n";
	foreach my $k (sort keys %totals) {
		print "## Total prof_$k: ", $totals{$k}, "\n";
	}
}
exit $total_err ? 1 : 0;

sub
write_obj($)
{
	my ($obj) = @_;
	if ($indented) {
		$obj->write_indented(\*STDOUT);
	} elsif ($parsed) {
		$obj->write(\*STDOUT);
	} else {
		print join("\n", $obj->getarray("RAW")), "\n";
	}
}

sub
do_query($)
{
	my $query = shift @_;
	my $q = Sherlock::Query->new("$host:$port");
	my $stat;

	if ($control) {
		$query = "control $query";
	}
	print "<<< $query\n" if $requests;
	$stat = $q->query($query, raw => !$parsed);
	$q->print if $debug;
	print "$stat\n" if $headers;
	if ($headers && $q->{HEADER}->get_attrs) {
		write_obj($q->{HEADER});
		print "\n";
	}
	if ($body) {
		foreach my $c (@{$q->{CARDS}}) {
			write_obj($c);
			print "\n";
		}
	}
	if ($footer && $q->{FOOTER}->get_attrs) {
		write_obj($q->{FOOTER});
		print "\n";
	}
	$total_ok++ if $stat =~ /^\+/;
	$total_err++ if $stat =~ /^-/;
	$total_cached++ if $q->{HEADER}->get('C');
	if (my $rt = $q->{FOOTER}->get('t')) { $total_time += $rt; }
	if (my $rT = $q->{FOOTER}->get('T')) { $rT =~ s/(\w+)=([0-9.]+)/$totals{$1}+=$2/ge; }
}
