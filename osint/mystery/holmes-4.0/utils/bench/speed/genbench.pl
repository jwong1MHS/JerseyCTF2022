#!/usr/bin/perl
# Generate data sets for speed benchmark from sherlockd/mux log files
# (c) 2003--2005 Martin Mares <mj@ucw.cz>

my $total = 0;
my $stats = 0;
my $nonimg = 0;
my $yesimg = 0;
my $muxctrl = 0;

while (<STDIN>) {
	chomp;
	/^I ....-..-.. ..:..:.. (\[\S+\] )?(\d+\.\d+\.\d+\.\d+ )?< (.*)/ || next;
	$_ = $3;
	if (/^MUXSTATUS\b/i) {
		$muxctrl++;
		next;
	}
	print "$_\n";
	$total++;
	/(^|\s)STATS /i and $stats++;
	if (/(^|\s)MUXSS \"img\"/i) {
		$yesimg++;
	} else {
		$nonimg++;
	}
}

print STDERR "Total queries: $total\n";
print STDERR "STATS only: $stats\n";
print STDERR "Images: yes $yesimg, no $nonimg\n";
print STDERR "Skipped MUX controls: $muxctrl\n";
