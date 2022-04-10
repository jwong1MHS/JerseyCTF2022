#!/usr/bin/perl
# Split search server or multiplexer log to individual queries
# (c) 2005 Martin Mares <mj@ucw.cz>

my %open = ();
while (<>) {
	my $pid, $type;
	if (($pid,$addr,$type) = /^. \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[(\d+)\] ([0-9.]+ )?([<>]) /) {
		if ($type eq "<") {
			if (exists $open{$pid}) { print STDERR "[$pid]: Duplicate entry $_\n"; }
			$open{$pid} = $_;
		} else {
			if (exists $open{$pid}) {
				print $open{$pid}, $_;
				delete $open{$pid};
			} elsif (!/ > -102/) {
				print STDERR "[$pid]: Missing start line for $_";
			}
		}
	}
}

foreach my $pid (keys %open) {
	print STDERR "[$pid] not closed\n";
}
