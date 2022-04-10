/*
   cole - A free C OLE library.
   Copyright 1998, 1999  Roberto Arturo Tena Sanchez

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
   Arturo Tena <arturo@directmail.org>
 */

#include "sherlock/sherlock.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if !(defined( __WIN32__ ) || defined( __BORLANDC__ ))
#include <unistd.h>
#endif

#include "internal.h"

extern u32 excel_stream_size;

int
__cole_extract_file (FILE **file, char **filename, U32 size, U32 pps_start,
		    U8 *BDepot, U8 *SDepot UNUSED, FILE *sbfile UNUSED, struct fastbuf *source)
{
	/* FIXME rewrite this cleaner */

	U16 BlockSize, Offset;
	U8 *Depot;
	struct fastbuf *infile;
	long long FilePos;
	size_t bytes_to_copy;
	U8 Block[0x0200];
	int ret;

	*filename = xmalloc(TMPNAM_LEN);
	if (*filename == NULL)
		return 1;

	strcpy(*filename, "tmp/xlHtmlXXXXXX");
	ret = mkstemp(*filename);
	if (ret == -1)	{
		xfree(*filename);
		return 2;
	}

	*file = fdopen(ret, "w+b");
	if (*file == NULL)	{
		xfree(*filename);
		close(ret);
		return 3;
	}
	/* unlink() is called so this file deletes when we are done with it */
	unlink(*filename);

	/* read from big block depot */
	Offset = 1;
	BlockSize = 0x0200;
	infile = source;
	Depot = BDepot;

	while (pps_start != 0xfffffffeUL /*&& pps_start != 0xffffffffUL &&
		pps_start != 0xfffffffdUL*/) {
		FilePos = ((long long)pps_start + Offset) * BlockSize;
		if (FilePos > 0x7fffffff)
		  gerror(2203, "Broken EXCEL file (illegal seek)");
		if (FilePos < 0) {
			fclose (*file);
			remove (*filename);
			xfree(*filename);
			return 4;
		}
		bytes_to_copy = MIN ((U32)BlockSize, size);
		excel_bseek (infile, FilePos, SEEK_SET);
		bread (infile, Block, bytes_to_copy);
		fwrite (Block, bytes_to_copy, 1, *file);
		if (ferror (*file)) {
			fclose (*file);
			remove (*filename);
			xfree(*filename);
			return 6;
		}
		pps_start = fil_sreadU32 (Depot + (pps_start * 4));
		size -= MIN ((U32)BlockSize, size);
		if (size == 0)
			break;
	}

	return 0;
}

void
excel_bseek(struct fastbuf *f, ucw_off_t pos, int whence)
{
  if(pos > excel_stream_size)
    gerror(2203, "Broken EXCEL file (illegal seek)");
  bseek(f, pos, whence);
}
