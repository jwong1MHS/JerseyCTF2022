#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Bucket file size";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $s = $pic->new_plot("Actual size (MB)");
my $a = $pic->new_plot("Space available (MB)");
my $e = $pic->new_plot("Space estimated (MB)");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($s, $r{'start_time'}, $r{'bucket_size'}) if defined $r{'bucket_size'};
	$pic->plot_value($a, $r{'start_time'}, $r{'space_avail'}) if defined $r{'space_avail'};
	$pic->plot_value($e, $r{'start_time'}, $r{'space_estim'}) if defined $r{'space_estim'};
});

$pic->draw_picture;

