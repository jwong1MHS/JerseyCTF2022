/*
 *	Parsing of Bit/Byte Streams
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#ifndef _MP3_BITSTREAM_H
#define _MP3_BITSTREAM_H

#include <setjmp.h>
#include <stdarg.h>

struct bit_stream {
	byte *start;
	byte *pos;						// Remaining bytes from the byte stream
	int remains;
	unsigned int part, rem;					// State of the bitstream reader
	void (*warn)(struct bit_stream *s, char *msg, va_list va);	// Report warnings (may be NULL)
	void (*err)(struct bit_stream *s, char *msg, va_list va);	// Report errors (may be NULL)
	void *priv;
	jmp_buf *err_jmp;					// Jump here after an error (may be NULL)
};

void bs_init(struct bit_stream *s, byte *start, uns len);
void bs_reset(struct bit_stream *s, byte *start, uns len);
void bs_error(struct bit_stream *s, char *msg, ...);
void bs_errorv(struct bit_stream *s, char *msg, va_list args);
void bs_warn(struct bit_stream *s, char *msg, ...);
void bs_warnv(struct bit_stream *s, char *msg, va_list args);
byte *bs_get(struct bit_stream *s, uns len, char *msg, ...);
byte *bs_pop(struct bit_stream *s, uns len, char *msg, ...);
int bs_subrange(struct bit_stream *outer, struct bit_stream *inner, uns len, char *msg, ...);
unsigned int bs_tell(struct bit_stream *s);

void bs_init_bits(struct bit_stream *s);
unsigned int bs_get_bits(struct bit_stream *s, unsigned int n);
unsigned int bs_tell_bit_pos(struct bit_stream *s);

#endif
