#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Queries: Query errors";
my $t = $pic->new_plot("AVG Query errors/sec");
my $total=0;

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$total += $r{'errors'};
	$pic->plot_value($t,$r{'start_time'}, $r{'errors'}/$quantum);
});

$pic->{INIT_CMD} .= "set label \"$total query errors\" at screen 0.15,0.9\n";
$pic->draw_picture;
