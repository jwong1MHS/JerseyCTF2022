/*
 *	Sherlock Library -- LiZaRd Streams
 *
 *	(c) 2004--2005, Robert Spalek <robert@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#ifndef _SHERLOCK_LIZARD_FB_H
#define _SHERLOCK_LIZARD_FB_H

#include "ucw/lizard.h"

struct fastbuf;

struct lizard_block_req
{			/* Input:			Output: */
  uns type;		/* desired bucket type		output bucket type (may be different: V33_LIZARD -> V33) */
  float ratio;		/* minimum compression ratio */
  byte *in_ptr;		/* input */
  uns in_len;		/* input size */
  byte *out_ptr;	/* output buffer		output data (can be shared with input) */
  int out_len;		/* output buffer size		real/needed output length */
};

int lizard_compress_req(struct lizard_block_req *req);
int lizard_compress_req_static(struct lizard_block_req *req);
  /* Return values:
   * 1: compressed and stored in the given buffer
   * 0: not compressed and the pointer to original data is passed back
   * -1: does not fit into buffer (the 2nd function never returns this,
   *  	 but it instead uses an internal gbuf)
   */
#define LIZARD_COMPRESS_HEADER	2
int lizard_compress_req_header(struct lizard_block_req *req, uns header_room);
  /* This function works like lizard_compress_req_static(), but in addition to that
   * it prepends a trivial header with bucket type and header/body separator.
   * Set header_room if there are LIZARD_COMPRESS_HEADER overwritable bytes
   * before the input buffer.
   */

void lizard_set_type(uns type, float min_compr);
int lizard_bwrite(struct fastbuf *fb_out, byte *ptr_in, uns len_in);
int lizard_bbcopy_compress(struct fastbuf *fb_out, struct fastbuf *fb_in, uns len_in);
int lizard_memread(struct lizard_buffer *liz_buf, byte *ptr_in, byte **ptr_out, uns *type);
int lizard_bread(struct lizard_buffer *liz_buf, struct fastbuf *fb_in, byte **ptr_out, uns *type);
  /* These functions use static variables, hence they are not re-entrant.  */

#endif
