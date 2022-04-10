/*
 *	Uncompress -- ported GPL source code
 *
 *	(c) 2001, Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/gather.h"

#include <string.h>

#define TRACE(x,y...) do { if (trace_decode) log(L_DEBUG, x,##y); } while (0)
#define XTRACE(x,y...) do { if (trace_decode > 1) log(L_DEBUG, x,##y); } while (0)

/*
 * The following code is taken from gzip-1.2.4 from the files gzip.h, lzw.h,
 * unlzw.c.  It is very hacked and the function unlzw is completely rewritten
 * using fastbuf streams.
 */

/* gzip.h -- common declarations for all gzip modules
 * Copyright (C) 1992-1993 Jean-loup Gailly.
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU General Public License, see the file COPYING.
 */

typedef unsigned char  uch;
typedef unsigned short ush;
typedef unsigned long  ulg;

#define INBUFSIZ  0x8000  /* input buffer size */
#define INBUF_EXTRA  64     /* required by unlzw() */

#define OUTBUFSIZ  16384  /* output buffer size */
#define OUTBUF_EXTRA 2048   /* required by unlzw() */

#define DIST_BUFSIZE 0x8000 /* buffer for distances, see trees.c */

#define WSIZE 0x8000     /* window size--must be a power of two, and at least 32K for zip's deflate method */

/* lzw.h -- define the lzw functions.
 * Copyright (C) 1992-1993 Jean-loup Gailly.
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU General Public License, see the file COPYING.
 */

#define BITS 16
#define INIT_BITS 9              /* Initial number of bits per code */

#define BIT_MASK    0x1f /* Mask for 'number of compression bits' */
	/* Mask 0x20 is reserved to mean a fourth header byte, and 0x40 is free.
	 * It's a pity that old uncompress does not check bit 0x20. That makes
	 * extension of the format actually undesirable because old compress
	 * would just crash on the new format instead of giving a meaningful
	 * error message. It does check the number of bits, but it's more
	 * helpful to say "unsupported format, get a new version" than
	 * "can only handle 16 bits".
	 */

#define BLOCK_MODE  0x80
	/* Block compression: if table is full and compression rate is dropping,
	 * clear the dictionary.
	 */

#define LZW_RESERVED 0x60 /* reserved bits */

#define	CLEAR  256       /* flush the dictionary */
#define FIRST  (CLEAR+1) /* first free entry */

/* unlzw.c -- decompress files in LZW format.
 * The code in this file is directly derived from the public domain 'compress'
 * written by Spencer Thomas, Joe Orost, James Woods, Jim McKie, Steve Davies,
 * Ken Turkowski, Dave Mack and Peter Jannesen.
 *
 * This is a temporary version which will be rewritten in some future version
 * to accommodate in-memory decompression.
 */

typedef	unsigned char	char_type;
typedef          long   code_int;
typedef unsigned long 	count_int;
typedef unsigned short	count_short;
typedef unsigned long 	cmp_code_int;

#define MAXCODE(n)	(1L << (n))

#  define input(b,o,c,n,m){ \
	char_type *p = &(b)[(o)>>3]; \
		(c) = ((((long)(p[0]))|((long)(p[1])<<8)| \
			((long)(p[2])<<16))>>((o)&0x7))&(m); \
			(o) += (n); \
}

#define tab_prefixof(i) tab_prefix[i]
#define clear_tab_prefixof()	bzero(tab_prefix, 256 * sizeof(*tab_prefix));

#define de_stack        ((char_type *)(&d_buf[DIST_BUFSIZE-1]))
#define tab_suffixof(i) tab_suffix[i]

/* ============================================================================
 * Decompress in to out.  This routine adapts to the codes in the
 * file building the "string" table on-the-fly; requiring no table to
 * be stored in the compressed file.
 * IN assertions: the buffer inbuf contains already the beginning of
 *   the compressed data, from offsets iptr to insize-1 included.
 *   The magic header has already been checked and skipped.
 *   bytes_in and bytes_out have been initialized.
 */
int
compress_parse(char **args UNUSED)
{
#define	ERR(nr, txt)	do { err=nr; err_msg=txt; goto bye; } while(0)
	struct fastbuf *in, *out;
	int err = 0;
	char *err_msg = NULL;
	int eof = 0;

	uch inbuf[INBUFSIZ+INBUF_EXTRA];	/* input buffer */
	uch outbuf[OUTBUFSIZ+OUTBUF_EXTRA];	/* output buffer */
	uch d_buf[DIST_BUFSIZE];		/* buffer for distances, see trees.c */
	uch window[WSIZE];			/* Sliding window and suffix table (unlzw) */
#define tab_suffix window
#define tab_prefix prev				/* hash link (see deflate.c) */
#define head (prev+WSIZE)			/* hash head (see deflate.c) */
	ush tab_prefix[(1<<BITS)];		/* prefix code (see unlzw.c) */

	int maxbits = BITS;			/* max bits per code for LZW */
	int block_mode = BLOCK_MODE;		/* block compress mode -C compatible with 2.0 */

	uns insize = 0;				/* valid bytes in inbuf */
	uns inptr = 0;				/* index of next byte to be processed in inbuf */
	uns bytes_in = 0;			/* number of input bytes */
	uns bytes_out = 0;			/* number of output bytes */

	char_type  *stackp;
	code_int   code;
	int        finchar;
	code_int   oldcode;
	code_int   incode;
	long       inbits;
	long       posbits;
	int        outpos;
	/*  int        insize; (global) */
	unsigned   bitmask;
	code_int   free_ent;
	code_int   maxcode;
	code_int   maxmaxcode;
	int        n_bits;
	int        rsize;

	in = fbmem_clone_read(gthis->contents);
	out = gthis->temp = fbmem_create(16384);

	insize = bread(in, inbuf, INBUFSIZ);
	TRACE("Uncompress: read %d bytes", insize);
	inptr = 3;

	if (insize<3
	|| inbuf[0] != 0x1f
	|| inbuf[1] != 0x9d)
		ERR(2500,"Uncompress: invalid header");
	maxbits = inbuf[2];
	block_mode = maxbits & BLOCK_MODE;
	if ((maxbits & LZW_RESERVED) != 0)
		log(L_WARN_R, "Uncompress: warning, unknown flags 0x%x", maxbits & LZW_RESERVED);
	maxbits &= BIT_MASK;
	maxmaxcode = MAXCODE(maxbits);
	TRACE("Uncompress: readed header");

	if (maxbits > BITS)
		ERR(2500, "Uncompress header error: more bits than I can handle");
	rsize = insize;
	maxcode = MAXCODE(n_bits = INIT_BITS)-1;
	bitmask = (1<<n_bits)-1;
	oldcode = -1;
	finchar = 0;
	outpos = 0;
	posbits = inptr<<3;

	free_ent = ((block_mode) ? FIRST : 256);

	clear_tab_prefixof(); /* Initialize the first 256 entries in the table. */

	for (code = 255 ; code >= 0 ; --code) {
		tab_suffixof(code) = (char_type)code;
	}
	do {
		int i;
		int  e;
		int  o;

resetbuf:
		e = insize-(o = (posbits>>3));

		for (i = 0 ; i < e ; ++i) {
			inbuf[i] = inbuf[i+o];
		}
		insize = e;
		posbits = 0;

		if (insize < INBUF_EXTRA) {
			if (!(rsize = bread(in, inbuf+insize, INBUFSIZ)))
				eof = 1;
			XTRACE("Uncompress: read %d bytes", rsize);
			insize += rsize;
			bytes_in += (ulg)rsize;
		}
		inbits = ((rsize != 0) ? ((unsigned long)insize - insize%n_bits)<<3 :
				((unsigned long)insize<<3)-(n_bits-1));

		while (inbits > posbits) {
			if (free_ent > maxcode) {
				posbits = ((posbits-1) +
						((n_bits<<3)-(posbits-1+(n_bits<<3))%(n_bits<<3)));
				++n_bits;
				if (n_bits == maxbits) {
					maxcode = maxmaxcode;
				} else {
					maxcode = MAXCODE(n_bits)-1;
				}
				bitmask = (1<<n_bits)-1;
				goto resetbuf;
			}
			input(inbuf,posbits,code,n_bits,bitmask);
			/* XTRACE("%d ", code); */

			if (oldcode == -1) {
				if (code >= 256)
					ERR(2501, "corrupt input.");
				outbuf[outpos++] = (char_type)(finchar = (int)(oldcode=code));
				continue;
			}
			if (code == CLEAR && block_mode) {
				clear_tab_prefixof();
				free_ent = FIRST - 1;
				posbits = ((posbits-1) +
						((n_bits<<3)-(posbits-1+(n_bits<<3))%(n_bits<<3)));
				maxcode = MAXCODE(n_bits = INIT_BITS)-1;
				bitmask = (1<<n_bits)-1;
				goto resetbuf;
			}
			incode = code;
			stackp = de_stack;

			if (code >= free_ent) { /* Special case for KwKwK string. */
				if (code > free_ent)
					ERR(2501, "corrupt input.");
				*--stackp = (char_type)finchar;
				code = oldcode;
			}

			while ((cmp_code_int)code >= (cmp_code_int)256) {
				/* Generate output characters in reverse order */
				*--stackp = tab_suffixof(code);
				code = tab_prefixof(code);
			}
			*--stackp =	(char_type)(finchar = tab_suffixof(code));

			/* And put them out in forward order */
			{
				int	i;

				if (outpos+(i = (de_stack-stackp)) >= OUTBUFSIZ) {
					do {
						if (i > OUTBUFSIZ-outpos) i = OUTBUFSIZ-outpos;

						if (i > 0) {
							memcpy(outbuf+outpos, stackp, i);
							outpos += i;
						}
						if (outpos >= OUTBUFSIZ) {
							bwrite(out, (char*)outbuf, outpos);
							TRACE("Uncompress: written %d bytes", outpos);
							bytes_out += (ulg)outpos;
							outpos = 0;
						}
						stackp+= i;
					} while ((i = (de_stack-stackp)) > 0);
				} else {
					memcpy(outbuf+outpos, stackp, i);
					outpos += i;
				}
			}

			if ((code = free_ent) < maxmaxcode) { /* Generate the new entry. */

				tab_prefixof(code) = (unsigned short)oldcode;
				tab_suffixof(code) = (char_type)finchar;
				free_ent = code+1;
			}
			oldcode = incode;	/* Remember previous code.	*/
		}
		if (max_decode_size && bytes_out >= max_decode_size)
		{
			log(L_WARN_R, "Cutting %d bytes long inflated file (maximum is %d)", bytes_out, max_decode_size);
			eof = 2;
		}
	} while (!eof);

	if (outpos > 0) {
		bwrite(out, (char*)outbuf, outpos);
		TRACE("Uncompress: written %d bytes", outpos);
		bytes_out += (ulg)outpos;
	}
	TRACE("Uncompress: complete (%d bytes long)", bytes_out);
	if (eof == 2)
		gobj_truncate();
bye:
	bclose(in);
	if (!err)
	{
		switch_content_encoding();
		return 0;
	}
	else
		gerror(err, err_msg ? : "Unknown error");
}
