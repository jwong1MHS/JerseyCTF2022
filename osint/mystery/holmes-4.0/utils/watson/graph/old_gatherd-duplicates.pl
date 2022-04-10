#!/usr/bin/perl -w

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Duplicates";
my $dup = $pic->new_plot("Duplicates %");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($dup,$r{'start_time'},100*(defined $r{'duplicate'} ? $r{'duplicate'} : 0)/$r{'total'}) if $r{'total'}>0;
});

$pic->draw_picture;

