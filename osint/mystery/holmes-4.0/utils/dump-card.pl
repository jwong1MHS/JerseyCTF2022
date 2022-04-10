#!/usr/bin/perl
#	Dumper of cards (might be specified in 7 ways)
#	(c) 2003--2007, Robert Spalek <robert@ucw.cz>
#	(c) 2004, Martin Mares <mj@ucw.cz>

use Getopt::Long;
use strict;
use warnings;
use integer;

use lib 'lib/perl5';
use Sherlock::Query();

my (@buckets, @urls, @bids, @cards, @patched, @URLs, @oids);
my $idxdir = "index";
my $host = "localhost";
my $port = 8192;
my $shep_host = "";
my $shep_dir = "~/run";
my $dumperparams = "";
my $verbose = 0;
my $decimal = 0;
my $to_stdout = 0;
my $dump = 1;
my $resolve = 0;
my $gatherer = 0;
my $shepherd = 0;
my $allbuckets = 0;
my $withlinks = 0;
my $withsimilar = 0;
my $withmerges = 0;
my $convert_id2oid = 0;
my $convert_oid2id = 0;

my $help = "Syntax: dump-card.pl [<options>] <descriptors>
Options:
	--dirindex DIRECTORY
	--host, --port	... of running sherlockd
	--shep-host, --shep-dir	... hostname/directory of remote Shepherd
	--dumperparams PARAMETERS FOR OBJDUMP
	--verbose	... prints the shell program to STDERR
	--decimal	... numbers are read in decimal
	--stdout	... do not use less
	--nodump	... do not actually dump, but do things around
	--resolve	... resolve BUCKETNR->ID and CARDID->OID
	--gatherer	... use v2.6 gatherer database instead of url-list
	--shepherd	... use v3.11 shepherd database instead of url-list
Descriptors:
  Bucket-file:
	--allbuckets
	--url URL
	--bucket BUCKETNR
	--id ID		... stage1 object ID, intervals are also allowed
	  --withlinks	... documents that point TO this document
	  --withsimilar
	  --withmerges
	  --withcards
  Cards:
	--turl URL
	--card CARDID	... stage2 card ID, all cards can be dumped by CARDID=0-
	--patched CARDID... fake object created by patch-index for stage2
	--oid OID	... stage2 object ID, intervals are also allowed
	  --withbuckets
";
GetOptions(
	"dirindex=s" => \$idxdir,
	"host=s" => \$host,
	"port=i" => \$port,
	"shep-host=s" => \$shep_host,
	"shep-dir=s" => \$shep_dir,
	"dumperparams=s" => \$dumperparams,
	"verbose!" => \$verbose,
	"decimal!" => \$decimal,
	"allbuckets!" => \$allbuckets,
	"stdout!" => \$to_stdout,
	"dump!" => \$dump,
	"resolve!" => \$resolve,
	"gatherer!" => \$gatherer,
	"shepherd!" => \$shepherd,
	"bucket=s@" => \@buckets,
	"url=s@" => \@urls,
	"turl=s@" => \@URLs,
	"id=s@" => \@bids,
	"withlinks!" => \$withlinks,
	"withsimilar!" => \$withsimilar,
	"withmerges!" => \$withmerges,
	"withcards!" => \$convert_id2oid,
	"card=s@" => \@cards,
	"patched=s@" => \@patched,
	"oid=s@" => \@oids,
	"withbuckets!" => \$convert_oid2id,
) || die $help;
die "Superfluous options (@ARGV)\n$help" if $#ARGV >= 0;

### Clean-up input lists
@buckets = map { split(/,?\s+/, $_) } @buckets;
@bids = map { split(/,?\s+/, $_) } @bids;
@cards = map { split(/,?\s+/, $_) } @cards;
@patched = map { split(/,?\s+/, $_) } @patched;
@oids = map { split(/,?\s+/, $_) } @oids;
@urls = map { split(/\s+/, $_) } @urls;		# XXX: not sure if this should be done, but my convenience tells me yes
@URLs = map { split(/\s+/, $_) } @URLs;
if ($decimal)
{
	@buckets = map { sprintf("%x", $_) } @buckets;
	@bids = map { sprintf("%x", $_) } @bids;
	@cards = map { sprintf("%x", $_) } @cards;
	@patched = map { sprintf("%x", $_) } @patched;
	@oids = map { sprintf("%x", $_) } @oids;
}

### Translate URL -> BUCKETNR -> ID and URL -> OID
if ($#urls >= 0)
{
	$" = ", "; print STDERR "urls: @urls\n";
	if ($gatherer) {
		@buckets = (@buckets, url2bucket(@urls));
	} elsif ($shepherd) {
		@buckets = (@buckets, shepurl2bucket(@urls));
	} else {
		@bids = (@bids, url2id(@urls));
	}
}
if ($#URLs >= 0)
{
	$" = ", "; print STDERR "URLs: @URLs\n";
	@oids = (@oids, URL2oid(@URLs));
}
if ($resolve)
{
	@bids = (@bids, search_position("Bucket", "attributes", "a", @buckets));
	@oids = (@oids, search_position("Card", "card-attrs", "d", @cards));
}
### Translate between ID <-> OID
$convert_id2oid and @oids = (@oids, translate_id2oid(@bids));
$convert_oid2id and @bids = (@bids, translate_oid2id(@oids));
### Add related documents from the 1st pass of the indexation
$withlinks and @bids = map { get_links($_) } @bids;
$withsimilar and @bids = map { get_similar($_) } @bids;
$withmerges and @bids = map { get_merges($_) } @bids;
### Compute positions in bucket-files
if ($#bids >= 0)
{
	$" = ", "; print STDERR "bids: @bids\n";
	@buckets = (@buckets, id2card("a", "attributes", @bids));
}
if ($#oids >= 0)
{
	$" = ", "; print STDERR "oids: @oids\n";
	@cards = (@cards, id2card("d", "card-attrs", @oids));
}
### Sort the dumped files
@buckets = make_unique(sort { hex $a <=> hex $b } @buckets);
@cards = make_unique(sort { hex $a <=> hex $b } @cards);
@patched = make_unique(sort { hex $a <=> hex $b } @patched);
### Prepare the dumping script
die "Nothing to dump\n$help" if ($#buckets < 0 and $#cards < 0 and $#patched < 0 and !$allbuckets);
my $program = "(\n";
if ($allbuckets)
{
	$program .= "echo \"### ffffffff 0 80000000\n#All buckets:\n\"\n";
	if ($shep_host ne "")
	{
		$program .= "ssh $shep_host '(cd $shep_dir; ";
		$program .= "./bin/buckettool -c\n";
		$program .= ")'\n";
	}
	else
	{
		$program .= "./bin/buckettool -c\n";
	}
}
if ($#buckets >= 0)
{
	$program .= "echo \"### ffffffff 0 80000000\n#Buckets:\n\"\n";
	$" = ", "; print STDERR "buckets: @buckets\n";
	my @buckets2 = map { "echo \"### $_ 0 0\"; ./bin/buckettool -x $_; echo" } @buckets;
	$" = "\n";
	if ($shep_host ne "")
	{
		$program .= "ssh $shep_host '(cd $shep_dir; ";
		$program .= "@buckets2\n";
		$program .= ")'\n";
	}
	else
	{
		$program .= "@buckets2\n";
	}
}
if ($#cards >= 0)
{
	$program .= "echo \"### ffffffff 0 80000000\n#Cards:\n\"\n";
	$" = ", "; print STDERR "cards: @cards\n";
	$" = " ";
	$program .= "./bin/idxdump -f $idxdir/cards -c @cards\n";
}
if ($#patched >= 0)
{
	$program .= "echo \"### ffffffff 0 80000000\n#Patched objects:\n\"\n";
	$" = ", "; print STDERR "patched: @patched\n";
	$" = " ";
	$program .= "./bin/idxdump -f $idxdir/objects -c @patched\n";
}
$program .= ") | ./bin/objdump $dumperparams";
$program .= " | less" if !$to_stdout;
$program .= "\n";
print STDERR $program if $verbose;
### And launch it
exec $program if $dump;

sub url2bucket
{
	my @urls = @_;
	$" = "\" -u\"";
	my $found = `./bin/gc -u\"@urls\"`;
	return map {
		if (/^([^ ]*) (?:\[[A-Z]*\] )*(.*)$/)
		{
			my $bucket = $2;
			if ($bucket =~ /^[0-9a-f]+$/)
			{ ($bucket) }
			else { () }	#can be, say, <unknown>
		} else { () }
	} split /\n/, $found;
}

sub shepurl2bucket
{
	my @urls = @_;
	$" = " ";
	if ($shep_host ne "")
	{
		$program .= "ssh $shep_host '(cd $shep_dir; ";
		$program .= "./bin/shep -l --url @urls";
		$program .= ")'\n";
	}
	else
	{
		$program .= "./bin/shep -l --url @urls";
	}
	my $found = `$program`;
	return map {
		if (/^[0-9a-f]*:[0-9a-f]*\s+([0-9a-f]*)/) { ($1) }
		else { () }
	} split /\n/, $found;
}

sub url2id
{
	my @urls = map { "\"$_\"" } @_;
	$" = " ";
	my @res = ();
	foreach my $z (`./bin/idxdump -f $idxdir/url-list -b -q @urls`) {
		$z =~ /^(\S+)\s+(.*)$/ or next;
		if ($1 ne "--------") {
			push @res, $1;
		} else {
			print STDERR "Unknown URL $2\n";
		}
	}
	return @res;
}

sub id2bucket
{
	my @ids = @_;
	$" = " ";
	return id2card("a", "attributes", @ids);
}

sub URL2oid
{
	my @URLs = @_;
	return map {
		my $q = Sherlock::Query->new("$host:$port");
		my $stat = $q->query("URL \"$_\"");
		if ($stat =~ /^\+/)
		{
			my $c = $q->{CARDS}->[0];
			if ($c)
			{ $c->get('O') }
			else { () }
		} else {
			printf STDERR "Cannot resolve $_: $stat\n";
			();
		}
	} @URLs;
}

sub id2card
{
	my ($par, $file, @ids) = @_;
	$" = " ";
	my $found = `./bin/idxdump -f $idxdir/$file -b -$par @ids`;
	return map {
		if (/^ *([0-9a-f]*) *([0-9a-f]*) /)
		{ $2 }
		else { () }
	} split /\n/, $found;
}

sub get_links
{
	my ($bid) = @_;
	my $renum = `./bin/idxdump -f $idxdir/link-graph-goes -m $bid`;
	$renum =~ /^0*$bid -> ([0-9a-f]*)$/ or return ($bid);
	my $graph_idx = `./bin/idxdump -f $idxdir/link-graph-index -G $1`;
	$graph_idx =~ /.* -> ([0-9a-f]*)/ or return ($bid);
	if ($1 eq "ffffffffff") { return ($bid); }
	my $target = `./bin/idxdump -f $idxdir/link-graph -g $1`;
	$target =~ /Vertex ([0-9a-f]*) /
	and hex $1 == hex $bid
		or return ($bid);
	return $bid, map {
		if (/<- ([0-9a-f]*)($| \[[fr])/)
		{ $1 }
		else { () }
	} split /\n/, $target;
}

sub get_similar
{
	my ($bid) = @_;
	my $similar = `grep "\\<$bid\t" $idxdir/matches`;
	return $bid, map {
		if (/^(\S*)\s*(\S*)\s*(\S*)$/)
		{
			my $id = ($1 eq $bid) ? $2 : $1;
			print STDERR "$bid similar to $id at level $3\n";
			$id;
		}
		else { () }
	} split /\n/, $similar;
}

sub get_merges
{
	my ($id) = @_;
	my $target = `./bin/idxdump -f $idxdir/merges -m $id`;
	$target =~ /^0*$id -> ([0-9a-f]*)$/ or return ($id);
	return ($id) if $1 eq "ffffffff";
	my $merged = `./bin/idxdump -f $idxdir/merges -m | grep " -> $1\$"`;
	return map {
		if (/^([0-9a-f]*) -> /)
		{ $1 }
		else { () }
	} split /\n/, $merged;
}

sub binary_search
{
	my ($file, $par, $search) = @_;
	$search = hex $search;
	my $a = 0, $b = 0x1fffffff;
	while ($a < $b)
	{
		my $c = ($a + $b)/2;
		my $cc = sprintf "%x", $c;
		my $line = `./bin/idxdump -f $idxdir/$file -b -$par $cc`;
		#print STDERR "$cc: $line";
		if ($line !~ /^\s*([0-9a-f]+)\s*([0-9a-f]+)/ or $1 ne $cc)
		{
			$b = $c;
			next;
		}
		my $pos = hex $2;
		return $cc if ($pos == $search);
		if ($pos < $search)
		{ $a = $c + 1; }
		else
		{ $b = $c; }
	}
	return "N/A";
}

sub search_position
{
	my ($text, $file, $par, @list) = @_;
	map {
		my $id = binary_search($file, $par, $_);
		print STDERR "$text $_ has id $id\n";
		$id eq "N/A" ? () : $id;
	} @list;
}

sub translate_id2oid
{
	my @ids = @_;
	map {
		my @bucketnr = id2bucket(($_));
		my $url = `./bin/buckettool -x $bucketnr[0] | grep ^U`;
		$url =~ s/^U(.*)\n/$1/;
		my @oid = URL2oid(($url));
		if ($#oid >= 0)
		{
			print STDERR "ID $_ -> OID @oid\n";
			$oid[0];
		} else {
			print STDERR "ID $_ has no OID\n";
			()
		}
	} @ids;
}

sub translate_oid2id
{
	my @oids = @_;
	map {
		my $oid = $_;
		my @cardnr = id2card("d", "card-attrs", ($oid));
		my @urls = `./bin/idxdump -f $idxdir/cards -c $cardnr[0] | grep ^[Uy]`;
		map {
			s/^[Uy](.*)\n/$1/;
			if ($gatherer or $shepherd) {
				my @bucketnr = $gatherer ? url2bucket($_) : shepurl2bucket($_);
				print STDERR "OID $oid <- BUCKETNR $bucketnr[0]\n";
				search_position("Bucket", "attributes", "a", @bucketnr);
			} else {
				url2id($_);
			}
		} @urls;
	} @oids;
}

sub make_unique
{
	my @a = ();
	foreach (@_) {
		if ($#a < 0 or hex $_ ne hex $a[$#a])
			{ $a[$#a+1] = $_; }
	}
	@a;
}
