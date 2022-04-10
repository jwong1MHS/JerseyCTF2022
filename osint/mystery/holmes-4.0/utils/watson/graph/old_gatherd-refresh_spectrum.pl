#!/usr/bin/perl -w

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Refresh spectrum";
my $sr = $pic->new_plot("Succesful refr. %");
$pic->new_plot("Soft err %",$sr,"1:3");
$pic->new_plot("Hard err %",$sr,"1:4");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	my $tr = (defined $r{'refresh'} ? $r{'refresh'} : 0)/100;
	if ($tr) {
		$pic->plot_value($sr, $r{'start_time'},
				(defined $r{'refr_ok'} ? $r{'refr_ok'} : 0)/$tr,
				(defined $r{'refr_err_soft'} ? $r{'refr_err_soft'} : 0)/$tr,
				(defined $r{'refr_err_hard'} ? $r{'refr_err_hard'} : 0)/$tr );
	}
}

