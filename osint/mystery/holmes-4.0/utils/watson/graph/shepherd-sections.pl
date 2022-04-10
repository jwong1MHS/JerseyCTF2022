#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Sections";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";

my %section = ();

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	for my $k (keys %r) {
		if ($k =~ /gathered_section_(\d+)/) {
			defined $section{$1} or $section{$1} = $pic->new_plot("Section $1 gathered");
			$pic->plot_value($section{$1}, $r{'start_time'}, $r{$k});
		}
	}
});

$pic->draw_picture;

