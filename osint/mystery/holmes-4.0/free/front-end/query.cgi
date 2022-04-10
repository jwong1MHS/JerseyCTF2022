#!/usr/bin/perl
# Example front-end CGI script for Sherlock
# Written by Martin Mares <mj@ucw.cz> and declared public domain

use strict;
use warnings;

use lib 'lib/perl5';
use IO::Scalar;
use Sherlock::Query;
use UCW::CGI;

# If you use a remote search engine, set the following variable:
my $search_engine = 'localhost:8192';

# Options (parsed by the CGI module)

my $query;	# User's query
my $magic;	# Magic debugging switch
my $show_first;	# First item to show
my $how_many;	# How many items to show

my %option_table = (
	'query' => { 'var' => \$query },
	'magic' => { 'var' => \$magic, 'check' => '\d+', 'default' => 0 },
	'first' => { 'var' => \$show_first, 'check' => '\d+', 'default' => 1 },
	'count' => { 'var' => \$how_many, 'check' => '\d+', 'default' => 10 }
);

# Prototypes of functions
sub page_top();
sub page_bottom();
sub page_status($);
sub page_error($);
sub page_result_top($);
sub page_result_card($$);
sub page_result_bottom($);
sub construct_query();

# The heart of the script
UCW::CGI::parse_args(\%option_table);
print "Content-type: text/html; charset=utf-8\n\n";
page_top();
if ($query ne "") {
	my $full_query = construct_query();
	print "<h3>Sending query:</h3><p><code>", html_escape($full_query), "</code>\n" if $magic;
	my $q = new Sherlock::Query($search_engine);
	my $res = $full_query ? $q->query($full_query) : "-999 Bad query syntax";
	page_status($q);
	if ($res =~ /^-/) {
		page_error($res);
	} else {
		page_result_top($q);
		foreach my $c (@{$q->{CARDS}}) {
			page_result_card($q, $c);
		}
		page_result_bottom($q);
	}
}
page_bottom();
exit 0;

# Print page header, style sheet and pre-filled query form
sub page_top() {
print <<EOF
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html40/loose.dtd">
<html><head>
<title>Sherlock Holmes Search Engine</title>
<style type="text/css">
/* Add something here */
</style>
</head><body>
<h1>Sherlock Holmes Search Engine</h1>

<form action="query.cgi">
EOF
;
$UCW::CGI::error_hook = sub { print "<h2>Internal bug: ", html_escape($_[0]), "</h2>\n"; };
print "<p>Find: <input name=query type=text maxlength=256 value='", html_escape($query), "'>\n";
print self_form('query' => undef, 'first' => undef);
print "<input type=submit></form>\n";
}

# Print page footer
sub page_bottom() {
print <<EOF
</html>
EOF
;
}

# Print search server error message
sub page_error($) {
	my ($err) = @_;
	my ($code,$msg) = $err =~ /^-(\d+)\s+(.*)/;
	print "<h2>Error: $msg</h2>\n";
}

# Print debugging output if requested
sub debug($$$) {
	my ($q, $what, $title) = @_;
	if ($magic) {
		print "<h3>$title</h3>" if $title;
		print "<pre>\n";
		my $s = "";
		my $io = new IO::Scalar \$s;
		$what->write_indented($io);
		print html_escape($s);
		print "</pre>\n";
	}
}

# Print general information on the query
sub page_status($) {
	my ($q) = @_;
	debug($q, $q->{HEADER}, "Response header:");
	debug($q, $q->{FOOTER}, "Response footer:");
}

# Print header of query results
sub page_result_top($) {
	my ($q) = @_;
	my $hdr = $q->{HEADER};
	my $ftr = $q->{FOOTER};
	print "<h3>Results of query ", html_escape($query), ":</h3>\n";
	my $db = $hdr->get('(D');	# assuming there is only a single database
	print "<p>Found ", $db->get('T'), " documents in ", $ftr->get('t')/1000, "s.\n";
	print "<ol start=$show_first>\n";
}

# find meta-information tagged with a given tag
sub find_meta($$) {
	my ($c, $meta) = @_;
	foreach my $m ($c->getarray('M')) {
		return $m if $m =~ /$meta/;
	}
	return "";
}

# format text snippet
sub format_snippet($) {
	my ($snip) = @_;
	$snip =~ s/<(\/?)([^>]*)>/
		"$1$2" eq "\/block" ? " ... " :
		$2 eq "best" ? "<$1b>" :
		$2 eq "found" ? "<$1b>" :
		""
	/ge;
	$snip =~ s/\s+\.\.\.\s*$//;
	$snip =~ s/\s\s+/ /g;
	return $snip;
}

# Print a single card of result
sub page_result_card($$) {
	my ($q, $c) = @_;
	my $url = $c->get('(U');				# use only the first URL block, ignore the rest
	my $URL = html_escape($url->get('U'));			# the URL itself, HTML-escaped
	my $title = find_meta($c, '<title>');			# page title (if any)
	print "<li><a href='$URL'>", $title ne "" ? format_snippet($title) : $URL, "</a>\n";
	if ($c->get('X')) {					# context (if any)
		print "<br>";
		print format_snippet(join(" ", $c->getarray('X')));
		print "\n";
	}
	print "<br><a href='$URL'>$URL</a>";			# URL
	print ", ", $url->get('s'), " bytes";			# size
	print ", ", $url->get('T');				# content-type
	print " (", $url->get('c'), ")" if defined $url->get('c');	# content-type and charset
	print ", Q=", $c->get('Q'), "\n";				# quality
	debug($q, $c, "");
}

# Print footer of query results
sub page_result_bottom($) {
	my ($q) = @_;
	print "</ol>\n";
	# navigation buttons
	print "<p>";
	my $num_replies = $q->{HEADER}->get('N');
	print "<a href='", self_ref('first' => $show_first-$how_many), "'>Prev</a>\n" if $show_first > $how_many;
	print "<a href='", self_ref('first' => $show_first+$how_many), "'>Next</a>\n" if $show_first+$how_many <= $num_replies;
	print "<a href='", self_ref('magic' => 1), "'>Debug</a>\n" if !$magic;
	print "<a href='", self_ref('magic' => 0), "'>bugeD</a>\n" if $magic;
}

# Transform user input to full query (see doc/search for syntax)
sub construct_query() {
	my $qry = "SHOW $show_first.." . ($show_first+$how_many-1) . " ";
	if ($query =~ /^\s*\?\s*(.*)/) {
		# "?" starts advanced queries
		$qry .= $1;
	} else {
		# Transform simple query to advanced query
		my @adv = ();
		$_ = $query;
		while (/^\s*(\+|-)?(("[^"]*")|[^" ]+)(.*)/) {
			$_ = $4;
			push @adv, ($1 ? ($1 eq "+" ? "" : "NOT ") : "MAYBE ") .
				   ($3 ? $3 : "\"$2\"");
		}
		if (!/^\s*$/) { return undef; }
		$qry .= join(" . ", @adv);
	}
	return $qry;
}
