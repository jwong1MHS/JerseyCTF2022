#!/usr/bin/perl

# Find url with cycles in url list (reads from stdin)
#
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

use strict;
use warnings;
use Getopt::Long;

my $help = 0;
my $count = 3;
my $urlout = 10;
my $sort = 0;
my %output = ();
my @sites = ();

GetOptions(
	"help" => \$help,
	"count=i" => \$count,
	"urls=i" => \$urlout,
	"sort!" => \$sort,
) and not $help or die "Usage: bin/find-cycles [<options>]
Reads url list from stdin
Options:
 --help		Print this help
 --count=	MinRepeatCount values for filter test(default 3)
 --urls=	Max urls printed per site(default 10)
 --sort		Sort sites by number of urls with cycles
";

-f "bin/filter-test" or die "bin/filter-test does not exist";
open COMMAND, "bin/filter-test -SURL.MinRepeatCount=$count cf/test-filter --shepherd --rejected --verdicts |" or die "Cannot execute bin/filter-test: $!";

while(<COMMAND>) {
	chomp;
	/^(http:\/\/(www.)?([^\/:]+).*): vicious circle$/ or next;
	if(not defined $output{$3}) {
		$output{$3} = {};
		${$output{$3}}{'cnt'} = 0;
		${$output{$3}}{'url'} = [];
	}
	push(@{${$output{$3}}{'url'}}, $1) if ${$output{$3}}{'cnt'}++ < $urlout;
}

if($sort) {
	@sites = sort { ${$output{$b}}{'cnt'} <=> ${$output{$a}}{'cnt'} } keys %output;
}
else {
	push @sites, keys %output;
}

foreach my $site (@sites) {
	print "$site\n";
	print ${$output{$site}}{'cnt'} . "\n";
	foreach my $url (@{${$output{$site}}{'url'}}) {
		print "$url\n"
	}
	print "\n";
}
