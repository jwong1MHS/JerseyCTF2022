#!/usr/bin/perl
# Find the call tree of memory allocations (using cscope)
# (c) 2004 Martin Mares <mj@ucw.cz>
# (c) 2006 Robert Spalek <robert@ucw.cz>

# The set of allocator calls we want to trace
my @queue = (
	"xmalloc", "xmalloc_zero", "ucw_xmalloc", "xfree", "free", "ucw_xfree",
	"mmap_file", "munmap_file",
	"mp_new", "mp_delete",
	"alloc_read_ary", "write_free_ary",
	"attrs_map", "attrs_unmap",
	"resolve_fastbuf",
	"bb_grow",
	#"partmap_open", "partmap_close",
	#"notes_part_map", "notes_part_unmap", "notes_skel_part_map", "notes_skel_part_unmap",
	#"attrs_part_map", "attrs_part_unmap",
);
my %known = map { $_ => 2 } @queue;

# Preprocess the source and remove gcc attributes, because they confuse cscope
my $src = join(" ", @ARGV);
`gcc -C -E -P -I. -Iobj $src | sed 's/__attribute__ *((.*))//g' >allocs.tmp`;
$? && die;
$src = "allocs.tmp";
unlink "cscope.out";
my %calls = ();

# Slurp file contents
open X, $src or die;
my @lines = <X>;
close X;

# Search the call graph backwards
while (@queue) {
	my $f = shift @queue;
	my @r = `cscope -I. -Iobj -L -3 $f $src`;
	$? && die "cscope failed";
	foreach my $l (@r) {
		my ($ffile, $ffunc, $fline, $fctxt) = ($l =~ /^(\S+)\s+(\S+)\s+(\S+)\s*(.*)/) or die;
		print "$f -> $ffunc ($ffile:$fline) $fctxt\n";
		exists $calls{$ffunc} or $calls{$ffunc} = [];
		push @{$calls{$ffunc}}, "$fline:$f";
		if (!exists $known{$ffunc}) {
			$known{$ffunc} = 1;
			push @queue, $ffunc;
		}
	}
}
print "\n";

my %mark = ();
sub look($$$)
{
	my ($f,$i,$l) = @_;
	my $ll = "";
	if ($l) {
		$ll = $lines[$l-1];
		chomp $ll;
		$ll =~ s/^\s+//;
		$ll =~ s/\s+$//;
		$ll = "{ $ll }";
	}
	print "$i$f";
	if ($known{$f} > 1) {
		$mark{$f} = 1;
		print " $ll\n";
	} elsif (exists $mark{$f}) {
		print " ... $ll\n";
	} else {
		$mark{$f} = 1;
		print " $ll\n";
		foreach my $z (sort @{$calls{$f}}) {
			my ($zl,$zf) = split(/:/,$z);
			look($zf, "$i  ", $zl);
		}
	}
}

look("main", "", "");
