#!/usr/bin/perl -w
# Watson statistics
# (c) 2005 Tomas Valla <tom@ucw.cz>
# (c) 2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Indexer: languages";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";

my %known_lang = ();

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	for my $k (keys %r) {
		if ($k =~ /lang_(\w+)/) {
			defined $known_lang{$1} or $known_lang{$1} = $pic->new_plot($1);
			$pic->plot_value($known_lang{$1}, $r{'start_time'}, $r{$k});
		}
	}
}

