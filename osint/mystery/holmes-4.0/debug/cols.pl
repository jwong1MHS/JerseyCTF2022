#!/usr/bin/perl

use strict;
use warnings;

my $i = 0;
my $prolog = "";
my $epilog = "";
my $each = "";
my @prints = ();
foreach $_ (@ARGV) {
	my ($f,$s) = /^(\d+)(:\w+)?$/ or die "Invalid field specification: $_";
	$f or die "Fields are numbered starting with 1";
	$f--;
	if (!defined $s) {
		push @prints, "defined(\$a[$f]) ? \$a[$f] : \"\"";
	} elsif ($s eq ":sum") {
		$prolog .= "my \$sum$i = 0;\n";
		$each .= "\$sum$i += \$a[$f] if \$a[$f];\n";
		$epilog .= "print \$sum$i, \"\\n\";\n";
	} elsif ($s eq ":avg") {
		$prolog .= "my (\$sum$i, \$cnt$i) = (0,0);\n";
		$each .= "if (defined \$a[$f]) { \$cnt$i++; \$sum$i += \$a[$f]; }\n";
		$epilog .= "print \$cnt$i ? (\$sum$i / \$cnt$i) : 0, \"\\n\";\n";
	} elsif ($s eq ":min") {
		$prolog .= "my \$min$i;\n";
		$each .= "if (defined \$a[$f] && (!defined(\$min$i) || \$a[$f] < \$min$i)) { \$min$i = \$a[$f]; }\n";
		$epilog .= "print \$min$i, \"\\n\";\n";
	} elsif ($s eq ":max") {
		$prolog .= "my \$max$i;\n";
		$each .= "if (defined \$a[$f] && (!defined(\$max$i) || \$a[$f] > \$max$i)) { \$max$i = \$a[$f]; }\n";
		$epilog .= "print \$max$i, \"\\n\";\n";
	} else {
		die "Invalid operator: $s";
	}
	$i++;
}

$each .= "print " . join(',"\t",', @prints) . ', "\n";' . "\n" if @prints;
my $prog = "
$prolog
while (<STDIN>) {
	chomp;
	my \@a = split /\\s+/;
	$each
}
$epilog";

# print "$prog";

eval $prog;
die $@ if $@;
