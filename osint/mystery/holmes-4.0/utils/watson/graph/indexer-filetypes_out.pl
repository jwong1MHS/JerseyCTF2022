#!/usr/bin/perl -w
# Watson statistics
# (c) 2005 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Indexer: filetypes out";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";

my %known_ft = ();

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	for my $k (keys %r) {
		if ($k =~ /ft_out_(\w+)/) {
			defined $known_ft{$1} or $known_ft{$1} = $pic->new_plot($1);
			$pic->plot_value($known_ft{$1}, $r{'start_time'}, $r{$k});
		}
	}
}

