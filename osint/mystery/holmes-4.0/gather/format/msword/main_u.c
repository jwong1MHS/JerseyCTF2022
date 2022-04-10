/*
 * main_u.c
 *
 * Released under GPL
 *
 * Copyright (C) 1998-2003 A.J. van Os
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * Description:
 * The main program of 'antiword' (Unix version)
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include <stdio.h>
#include <stdlib.h>
#if defined(__dos)
#include <fcntl.h>
#include <io.h>
#endif /* __dos */
#if defined(__STDC_ISO_10646__)
#include <locale.h>
#endif /* __STDC_ISO_10646__ */
#if defined(N_PLAT_NLM)
#if !defined(_VA_LIST)
#include "NW-only/nw_os.h"
#endif /* !_VA_LIST */
#include "getopt.h"
#endif /* N_PLAT_NLM */
#include "version.h"
#include "antiword.h"

static BOOL
bProcessFile(struct fastbuf *pFile, struct fastbuf *out)
{
	diagram_type	*pDiag;
	long		lFilesize;
	int		iWordVersion;
	BOOL		bResult;

	lFilesize = bfilesize(pFile);
	if (lFilesize < 0) {
		werr(0, "I can't get the size of input stream");
                bclose(pFile);
		return FALSE;
	}

	iWordVersion = iGuessVersionNumber(pFile, lFilesize);
	if (iWordVersion < 0 || iWordVersion == 3) {
		if (bIsRtfFile(pFile)) {
			werr(0, "Input stream is not a Word Document."
				" It is probably a Rich Text Format file");
		} if (bIsWordPerfectFile(pFile)) {
			werr(0, "Input stream is not a Word Document."
				" It is probably a Word Perfect file");
		} else {
#if defined(__dos)
			werr(0, "Input stream is not a Word Document or the filename"
				" is not in the 8+3 format");
#else
			werr(0, "Input stream is not a Word Document");
#endif /* __dos */
		}
                bclose(pFile);
		return FALSE;
	}
	/* Reset any reading done during file testing */
	brewind(pFile);

	pDiag = pCreateDiagram(out);
	if (pDiag == NULL) {
                bclose(pFile);
		return FALSE;
	}

	bResult = bWordDecryptor(pFile, lFilesize, pDiag);
	vDestroyDiagram(pDiag);

        bclose(pFile);
	return bResult;
} /* end of bProcessFile */

int
msword_parse(char **args UNUSED)
{
  options_type tOptions;
  int argc= 3;
  char *argv[]= {"not-used", "-s", "-w0"}; /* show hidden, not wrap lines */
  struct fastbuf *in, *out, *final;
  
  iReadOptions(argc, argv);
  vGetOptions(&tOptions);

  if(gthis->truncated)
    gerror(2205, "MSWORD file truncated");

  in = fbmem_clone_read(gthis->contents);
  final = gthis->text = fbmem_create(16384);
  out = fb_wrap_textpacker_out(final);
  if(!bProcessFile(in, out))
    gerror(2203, "AntiWord internal error");
  bclose(out);    
  return 1;
}
