#!/usr/bin/perl
# The first try on detecting duplicate servers by MM & SR

use strict;
use warnings;

print STDERR "Parsing merges\n";
my %merges = ();
open M, "bin/idxdump -m |" or die;
while (<M>) {
	/^(........) -> (........)/ or die;
	$merges{$1} = $2;
}
close M;

print STDERR "Parsing flags\n";
my %flags = ();
open A, "bin/idxdump -a |" or die;
$_ = <A>;
while (my $a = <A>) {
	$a =~ s/^\s+//;
	my ($aid, $acard, $asite, $aarea, $awgt, $aflags, $aage, $atype) = split(/\s+/, $a);
	$aid = "00000000$aid";
	$aid =~ s/^.*(........)/$1/;
	$flags{$aid} = $aflags;
}
close A;

print STDERR "Parsing URLs\n";
open U, "index/url-list" or die;
my %urls = ();
my %idtohost = ();
my %hosts = ();
my $id = 0;
while (my $url = <U>) {
	chomp $url;
	my ($host, $rest) = ($url =~ /^http:\/\/([^\/]+)\/(.*)/) or die;
	my $hexid = sprintf("%08x", $id);
	$id++;
	$urls{$url} = $hexid;
	$idtohost{$hexid} = $host;
	$hosts{$host} = 1;
}
close U;

print STDERR "Processing hosts\n";
my %eq = ();
foreach my $h (keys %hosts) {
	my $url = "http://$h/";
	if (!defined $urls{$url}) {
#		print "$url: not found\n";
		next;
	}
	my $id = $urls{$url};
	my $class = $merges{$id};
	if ($flags{$id} =~ /^E/) {
		if ($class eq "ffffffff") {
#			print "$url: empty with no destination\n";
			next;
		}
		$id = $merges{$id};
		if ($idtohost{$id} ne $h) {
#			print "$url: redirect to different host $idtohost{$id}\n";
			next;
		}
#		print "$url -> $id\n";
		if ($flags{$id} =~ /^E/) {
#			print "$url: Redirect to empty page\n";
			next;
		}
		$class = $merges{$id};
	}
	($class eq "ffffffff") && die "$url: invalid merge class";
#	print "$url : $id $class\n";
	if (!defined $eq{$class}) {
		$eq{$class} = $h;
	} else {
		$eq{$class} .= " $h";
	}
}

$" = " ";
sub make_uniq;
open FO, ">dups.txt" or die;
open FO2, ">cf/url-equiv.new" or die;
print STDERR "Dumping to dups.txt and cf/url-equiv.new\n";
foreach my $c (keys %eq) {
	if ($eq{$c} =~ / /) {
		my @x = make_uniq sort map { s/^(www\.)+//; $_ } split(/ /, $eq{$c});
		next if @x < 2;
		print FO "$c @x\n";
		print FO2 "http://$x[0]/\n";
		shift @x;
		foreach my $u (@x)
		{
			print FO2 "\thttp://$u/\n";
		}
	}
}
close FO;
close FO2;
print STDERR "Done\n";

sub make_uniq
{
	my @a = ();
	foreach (@_)
	{
		if ($#a < 0 or $_ ne $a[$#a])
			{ $a[$#a+1] = $_; }
	}
	return @a;
}
