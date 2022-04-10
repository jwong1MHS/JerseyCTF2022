#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Queries: Cache efficiency";
my $t = $pic->new_plot("Cache hits %");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($t,$r{'start_time'}, 100*$r{'cache_hits'}/$r{'total_qrs'}) if $r{'total_qrs'}>0;
});

$pic->draw_picture;
