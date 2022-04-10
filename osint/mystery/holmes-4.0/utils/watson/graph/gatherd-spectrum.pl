#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Spectrum";
my $dwn = $pic->new_plot("Downloads %");
$pic->new_plot("Redirects %",$dwn,"1:3");
$pic->new_plot("Resolves %",$dwn,"1:4");
$pic->new_plot("Soft errs %",$dwn,"1:5");
$pic->new_plot("Hard errs %",$dwn,"1:6");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	my $t = ((defined $r{'total'}) ? $r{'total'} : 0)/100;
	if ($t) {
		$pic->plot_value($dwn, $r{'start_time'},
				((defined $r{'download'}) ? $r{'download'} : 0)/$t,
				((defined $r{'redirect'}) ? $r{'redirect'} : 0)/$t,
				((defined $r{'resolve'}) ? $r{'resolve'} : 0)/$t,
				((defined $r{'err_soft'}) ? $r{'err_soft'} : 0)/$t,
				((defined $r{'err_hard'}) ? $r{'err_hard'} : 0)/$t );
	}
}

