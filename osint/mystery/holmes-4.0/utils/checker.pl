#!/usr/bin/perl -w
# Web Checker: Generating Statistics (see doc/hints for explanation)
# (c) 2002 Martin Mares <mj@ucw.cz>

use strict;
no strict 'vars';
use warnings;
use lib 'lib/perl5';
use UCW::Config();

my $analyse_log = "log/checker";
my $report_redir_links = 0;
my $limit_by_regex = ".*";

$UCW::Config::DefaultConfigFile = "cf/checker";
$UCW::Config::Sections{'Checker'} =
	{ 'AnalyseLog' => \$analyse_log,
	  'ReportRedirectLinks' => \$report_redir_links,
	  'LimitByRegex' => \$limit_by_regex
	};

UCW::Config::Parse(
) || die "Syntax: checker [<standard-options>]";

open LOG, $analyse_log or die "log/checker: $!";
my (%notes_by_url, %notes_by_pid);
while (<LOG>) {
	chomp;
	($cat,$_) = /^(.) ....-..-.. ..:..:.. (.*)$/ or die "Log parse error at $_";
	if ((($id,$r) = /^\[(\d+)\] (.*)$/) && $cat ne "D") {
		defined $notes_by_pid{$id} or $notes_by_pid{$id} = [];
		push @{$notes_by_pid{$id}}, $r;
	} elsif (($url,$code,$msg,$pid) = /^(.*): (\d{4}) (.*) \[(\d+)/) {
		if (defined $notes_by_url{$url}) {
			push @{$notes_by_url{$url}}, "Processed multiple times";
		} else {
			if ($notes_by_pid{$pid}) { $notes_by_url{$url} = $notes_by_pid{$pid}; }
			$status{$url} = "$code $msg";
		}
		delete $notes_by_pid{$pid};
	}
}
close LOG;

open BUCK, "bin/buckettool -c|" or die "Unable to dump buckets";
<BUCK>;
for(;;) {
	my %attr;
	while (<BUCK>) {
		chomp;
		($t,$v) = /^(.)(.*)$/;
		($t eq "#") && last;
		if (defined $attr{$t}) {
			$attr{$t} .= "\n$v";
		} else {
			$attr{$t} = $v;
		}
	}
	keys %attr or last;
	$url = $attr{"U"};
	$url =~ $limit_by_regex or next;
	$verdict = "";
	if ((($code,$msg) = ($status{$url} =~ /^(....) (.*)/)) && $code ne "0000") {
		$verdict .= "$msg\n";
	}
	my %refs;
	foreach $rt ("A", "F", "I", "R", "Y", "d", "f") {
		if (defined $attr{$rt}) {
			foreach $ref (split(/\n/, $attr{$rt})) {
				$ref =~ /^([^ ]+)/;
				$refs{$1} = 1;
			}
		}
	}
	foreach $ref (sort keys %refs) {
		if ($status{$ref} && $status{$ref} !~ /^0000 /) {
			!$report_redir_links && $status{$ref} =~ /^0001 / && next;
			$verdict .= "Link to $ref: " . $status{$ref} . "\n";
		}
	}
	if ($attr{"j"} && $attr{"j"} ne "# OK") {
		$verdict .= $attr{"j"} . "\n";
	}
	if ($verdict ne "") {
		print "### $url\n";
		print $verdict;
		print "\n";
	}
}
close BUCK;
