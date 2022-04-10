#!/usr/bin/perl -w

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Average queue priority";
my $qp = $pic->new_plot("Avg queue priority");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($qp,$r{'start_time'}, (defined $r{'queue_prior_sum'} ? $r{'queue_prior_sum'} : 0)/$r{'total'}) if $r{'total'}>0;
});

$pic->draw_picture;

