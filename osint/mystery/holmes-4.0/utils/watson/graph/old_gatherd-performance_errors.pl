#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Performance: Errors";
my $t = $pic->new_plot("Redirects/sec");
$pic->new_plot("Duplicates/sec",$t,"1:3");
$pic->new_plot("Soft errors/sec",$t,"1:4");
$pic->new_plot("Hard errors/sec",$t,"1:5");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($t, $r{'start_time'},
			(defined $r{'redirect'} ? $r{'redirect'} : 0)/$quantum,
			(defined $r{'duplicate'} ? $r{'duplicate'} : 0)/$quantum,
			(defined $r{'err_soft'} ? $r{'err_soft'} : 0)/$quantum,
			(defined $r{'err_hard'} ? $r{'err_hard'} : 0)/$quantum );
});

$pic->draw_picture;
