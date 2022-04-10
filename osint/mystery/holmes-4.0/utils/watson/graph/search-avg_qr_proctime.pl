#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Queries: Average query time";
my $t = $pic->new_plot("Average query time (milisec)");
my ($first, $last);
my ($tq,$tt,$te) = (0,0,0);

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$first = $r{'start_time'} if not defined $first;
	$last = $r{'start_time'};
	$tq += $r{'total_qrs'};
	$tt += $r{'total_qr_time'};
	$te += $r{'errors'};
	if ($r{'total_qrs'}-$r{'errors'}>0) {
		$pic->plot_value($t,$r{'start_time'}, $r{'total_qr_time'}/($r{'total_qrs'}-$r{'errors'}));
	}
});

if (defined $last && $last!=$first && $tq-$te>0) {
	my $n = $tt/($tq-$te);
	my $a = $pic->new_plot("final AVG ".sprintf("%.3g",$n)." ms");
	$pic->plot_value($a,$first,$n);
	$pic->plot_value($a,$last,$n);
}

$pic->draw_picture;
