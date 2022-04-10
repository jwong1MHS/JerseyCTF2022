#!/usr/bin/perl
# Grep split search server/mux logs
# (c) 2006 Martin Mares <mj@ucw.cz>

@ARGV == 2 or die "Usage: log-qgrep <query-regex> <status-regex>";
my $qr = $ARGV[0];
my $ar = $ARGV[1];

while (my $q = <STDIN>) {
	my $a = <STDIN>;
	($q =~ / < /) && ($a =~ / > /) || die;
	($q =~ $qr) && ($a =~ $ar) || next;
	print $q, $a;
}
