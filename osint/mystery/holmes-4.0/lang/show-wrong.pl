#!/usr/bin/perl
#	Display documents the language-detection has failed on
#	(use for debugging purposes only)
#	(c) 2003, Robert Spalek <robert@ucw.cz>

while (<>)
{
	@a = split /\s+/;
	if ($a[1] ne $a[3]
	and $a[3] ne "--"
	and $a[1] ne "??")
	{
		print;
	}
}
