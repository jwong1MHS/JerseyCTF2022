#	Perl module for sending queries to Sherlock search servers and parsing answers
#
#	(c) 2002--2007 Martin Mares <mj@ucw.cz>
#
#	This software may be freely distributed and used according to the terms
#	of the GNU Lesser General Public License.

=head1 NAME

Sherlock::Query -- Communication with Sherlock Search Servers

=head1 DESCRIPTION

This Perl library offers a simple interface for connecting to Sherlock
search servers, sending queries or control commands and parsing the
results.

First of all, you have to use

	my $conn = new Sherlock::Query('server:port');

to create a new connection object (unconnected yet). Then you can call

	my $res = $conn->query('"simple" OR "query"');

to establish the connection, send a given command to the search server
and parse the results (see below). The same interface can be used for
sending control commands to the search server.

A single connection object can be used for sending multiple queries.

=head1 RESULTS

The results of a query consist of several Sherlock objects as represented by the
L<Sherlock::Object> module: a header (stored in C<< $conn->{HEADER} >>), a
footer (stored in C<< $conn->{FOOTER} >>) and an array of index cards
(documents matching the query, stored in C<< $conn->{CARDS} >>).

See F<doc/search> for a description of these objects and also of the query
language.

When in doubt, call the C<print> method which will print the whole contents
of the connection object.

=head1 OPTIONS

You can also add options of type I<< key >> C<< => >> I<< value >> after the two
fixed arguments of the C<< query >> function. Currently, the only option defined
is C<< raw >>, which requests a raw (unparsed) version of each object output
by the search server to be added as its C<< RAW >> attribute.

=head1 SEE ALSO

A good example of use of this module is the C<query> utility and
of course the example front-end (F<front-end/query.cgi>).

=head1 AUTHOR

Martin Mares <mj@ucw.cz>

=cut

package Sherlock::Query;

use strict;
use warnings;
use IO::Socket::INET;
use Sherlock::Object;

sub new($$) {
	my ($class, $server) = @_;
	my $self = {
		SERVER	=> $server
	};
	bless $self;
	return $self;
}

sub parse($$$@) {
	my $q = shift @_;
	my $fh = shift @_;
	my %opts = @_;
	my $raw = ($opts{raw} ? 1 : 0);
	my $read = ($opts{read} ? $opts{read} : sub { my $fh = shift; return $_ = <$fh>; } );

	$q->{HEADER} = new Sherlock::Object;
	$q->{FOOTER} = new Sherlock::Object;
	$q->{CARDS} = [];

	# Status line
	my $stat = $read->($fh);
	$stat = "-903 Incomplete reply" if !defined $stat;
	chomp $stat;
	$stat =~ /^[+-]/ or return "-901 Reply status parse error";

	# Blocks of output
	my @ans = ();
	for(;;) {
		my $obj = new Sherlock::Object;
		my $res = $obj->read($fh, raw => $raw, read => $read);
		defined($res) or return "-901 Reply object parse error";
		$res or last;
		push @ans, $obj;
	}

	if ($#ans >= 0) {
		$q->{HEADER} = shift @ans;
	}
	if ($#ans >= 0) {
		$q->{FOOTER} = pop @ans;
	}
	$q->{CARDS} = \@ans;

	if (($stat =~ /^[+]/) || $q->{HEADER}) {
		# Non-trivial replies must end with the "+++" trailer
		my $ok = 0;
		foreach my $t ($q->{FOOTER}->getarray("+")) {
			# There can be also an empty "+" attribute signalizing the start of the footer
			$ok = 1 if $t eq "++";
		}
		$ok or return "-902 Incomplete reply, malformed trailer";
	}

	return $stat;
}

sub query($$@) {
	my $q = shift @_;
	my $cmd = shift @_;
	my $sock = IO::Socket::INET->new(PeerAddr => $q->{SERVER}, Proto => 'tcp')
		or return "-900 Cannot connect to search server: $!";
	print $sock $cmd, "\n";
	return $q->parse($sock, @_);
}

sub print($) {
	my ($q) = @_;
	foreach my $key (keys %$q) {
		print "$key => ";
		my $v = $q->{$key};
		my $t = ref $v;
		if ($t eq "") {
			if (defined $v) { print "$v\n"; }
			else { print "UNDEF\n"; }
		} elsif ($t eq "ARRAY") {
			print "[\n";
			foreach my $o (@$v) {
				$o->write_indented(\*STDOUT, "\t");
			}
			print "]\n";
		} elsif ($t eq "Sherlock::Object") {
			print "\n";
			$v->write_indented(\*STDOUT, "\t");
		} else {
			print "??? type $t ???\n";
		}
	}
}

1;  # OK
