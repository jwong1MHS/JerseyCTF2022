#!/usr/bin/perl
# Find and summarize gatherer errors
# (c) 2004 Martin Mares <mj@ucw.cz>

$recs = 0;
while (<>) {
	my ($url, $rc, $msg) = /^I ....-..-.. ..:..:.. (.*): (\d{4}) (.*) \[.*\]/ or next;
	$stats{$rc}++;
	$msgs{$rc}{$msg}++;
	if ($rc >= 1000 && $stats{$rc} < 10000) {
		if ($msgs{$rc}{$msg} < 10 || $rc == 1301 || $rc == 2301) {
			$urls{$rc}{$msg}{$url} = 1;
		}
	}
	$recs++;
}

print "    Count Code Typical messages\n";
foreach my $rc (sort keys %stats) {
	printf "%9d %s ", $stats{$rc}, $rc;
	my $c = 0;
	foreach my $m (sort { $msgs{$rc}{$b} <=> $msgs{$rc}{$a} } keys %{$msgs{$rc}} ) {
		$c++ && print "               ";
		if ($c > 50) {
			print "...\n";
			last;
		}
		print "$m ($msgs{$rc}{$m})\n";
		foreach my $u (keys %{$urls{$rc}{$m}}) {
			print "\t\t\t$u\n";
		}
	}
}
print "Total: $recs\n";
