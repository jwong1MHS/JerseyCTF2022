#!/usr/bin/perl
# Analyse queue keys and report oversize ones

#open X, "bin/gc -h |" or die "gc -h failed";
open X, "/tmp/1";
while (<X>) {
	($name, $c1, $c2, $c3, $key) = /^([^ ]*).* \[(\d+)\+(\d+)\+(\d+).* objects\].*\[key (........)\]/ or die "syntax error: $_";
	if (!defined $ctrs{$key}) {
		$ctrs{$key} = 0;
		$hctr{$key} = 0;
		$hosts{$key} = [];
	}
	$ctrs{$key} += ($c = $c1 + $c2 + $c3);
	$hctr{$key}++;
	$nc{$name} = $c;
	push @{$hosts{$key}}, $name;
}
close X;

$threshold = 28*86400/30;
print "# Threshold set to $threshold\n";

print "# docs\tqueuekey\thosts\tIP addr\tname\n";
foreach $k (sort { $ctrs{$b} <=> $ctrs{$a} } keys %ctrs) {
	$ctrs{$k} >= $threshold or last;
	$k =~ /^7f/ and next;
	@l = ($k =~ /(..)(..)(..)(..)/);
	$ip = join('.', map { hex $_; } @l);
	$name = `host $ip`;
	if ($name =~ /domain name pointer (.*)/) {
		$name = $1;
	} else {
		$name = "???";
	}
	print "$ctrs{$k}\t$k\t$hctr{$k}\t$ip\t$name\n";
	$cc=0;
	foreach $z (sort { $nc{$b} <=> $nc{$a} } @{$hosts{$k}}) {
		$cc++ <= 10 or last;
		print "\t$nc{$z}\t$z\n";
	}
}
