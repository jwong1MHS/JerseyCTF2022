#!/usr/bin/perl -w
#
# Report sites and qkeys which exceed limits
#
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
#
# Usage: bin/check-limit [--warn] [--qkey-only] [--site-only]

use Getopt::Long;
use strict;

my $warn = 0;
my $qkey_only = 0;
my $site_only = 0;
my $warn_level = 0.95;

my @input = ();
my @qkey_output = ();
my @site_output = ();
my @qkey_warn_output = ();
my @site_warn_output = ();

my $qkey_header = '';
my $site_header = '';

GetOptions(
	"warn!" => \$warn,
	"qkey-only" => \$qkey_only,
	"site-only" => \$site_only,
) or die "Usage: bin/check-limit [<options>]

Options:
 --warn		Show also records, which are near exceeding limits.
 --qkey-only	Show only records exceeding qkey limits.
 --site-only	Show only records exceeding sites limits.
";

# Qkey
if(!$site_only) {
	open COMMAND, 'bin/shep --qkey-stats --all |';

	$_ = <COMMAND>;
	if(/^Qkey\s+Sites\s+Active\s+Inactive\s+Delay\s+SoftLimit\s+HardLimit$/) {
		chomp;
		$qkey_header = $_;
		while(<COMMAND>) {
			chomp;
			/^([0-9a-f:]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$/ or next;
			if($3 >= $6 || $4 >= $7 ) {
				push @qkey_output, $_;
			}
			elsif($warn && ($3 >= $warn_level * $6 || $4 >= $warn_level * $7)) {
				push @qkey_warn_output, $_;
			}
		}
	}
	else {
		print "Format change, rewrite me !!!\n";
	}
	close COMMAND;
}

# Site
if(!$qkey_only) {
	open COMMAND, 'bin/shep --sites-filter --all |';

	$_ = <COMMAND>;
	if(!$qkey_only && /^Site footprint\s+Active\s+Inact\.\s+Gathrd\s+SKey\s+SoftLim\s+HardLim\s+FrshLim\s+MinD\s+QBonus\s+SBonus\s+AvT\s+Fil\s+Name$/) {
		chomp;
		$site_header = $_;
		while(<COMMAND>) {
			chomp;
			/^([0-9a-f]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([0-9a-f]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\S+\s+(.+)$/ or next;
			if($6 > 0 && $7 > 0 && ($2 >= $6 || $3 >= $7)) {
				push @site_output, $_;
			}
			elsif($warn && $6 > 0 && $7 > 0 && ($2 >= $warn_level * $6 || $3 >= $warn_level * $7)) {
				 push @site_warn_output, $_;
			}
		}
	}
	else {
		print "Format change, rewrite me !!!\n";
	}
}

# Output
$" = "\n";
if($warn) {
	print("$qkey_header\n@qkey_output\n" . "-" x 80 . "\n@qkey_warn_output\n\n\n") if not $site_only;
	print("$site_header\n@site_output\n" . "-" x 80 . "\n@site_warn_output\n") if not $qkey_only;
}
else {
	print("$qkey_header\n@qkey_output\n\n\n") if not $site_only;
	print("$site_header\n@site_output\n") if not $qkey_only;
}
