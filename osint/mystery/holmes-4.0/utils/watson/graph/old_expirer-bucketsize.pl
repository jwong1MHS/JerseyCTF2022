#!/usr/bin/perl -w

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Expirer: Bucket file size";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $t = $pic->new_plot("Bucket file size (MB)");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($t,$r{'start_time'}, $r{'size_objs'}/(1024*1024)) if defined $r{'size_objs'};
});

$pic->draw_picture;
