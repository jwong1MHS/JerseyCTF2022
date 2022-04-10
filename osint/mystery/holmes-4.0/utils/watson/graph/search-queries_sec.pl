#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Queries: Queries/sec";
my $t = $pic->new_plot("Queries/sec");
my ($first, $last);
my $total = 0;

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$first = $r{'start_time'} if not defined $first;
	$last = $r{'start_time'};
	$total += $r{'total_qrs'};
	$pic->plot_value($t,$r{'start_time'}, $r{'total_qrs'}/$quantum);
});

if (defined $last && $last!=$first) {
	my $n = $total/($last-$first);
	my $a = $pic->new_plot("final AVG ".sprintf("%.3g",$n));
	$pic->plot_value($a,$first,$n);
	$pic->plot_value($a,$last,$n);
}

$pic->draw_picture;
