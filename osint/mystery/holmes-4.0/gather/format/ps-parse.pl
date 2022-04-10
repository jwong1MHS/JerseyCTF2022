#!/usr/bin/perl
#	A very rough post-processor for pstotext from
#	http://www.research.compaq.com/SRC/virtualpaper/pstotext.html
#
#	(c) 2003, Robert Spalek <robert@ucw.cz>

use open IN => ":raw";

open(fi, "pstotext @ARGV |");
while (<fi>)
{
	s//ff/g;
	s//fi/g;
	s/\r/fl/g;
	s//ffl/g;
	s/\377/-/g;
	s/\306/ffi/g;
	s/[\x00-\x08\x0b\x0e-\x1f\x7f]/?/g;
	print;
}
close(fi);
