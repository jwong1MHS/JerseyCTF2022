/*
 *	Parsing of Bit/Byte Streams
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "bitstream.h"

#include <string.h>

/*** BYTE STREAMS ***/

void
bs_reset(struct bit_stream *s, byte *start, uns len)
{
	s->start = s->pos = start;
	s->remains = len;
}

void
bs_init(struct bit_stream *s, byte *start, uns len)
{
	bzero(s, sizeof(*s));
	bs_reset(s, start, len);
}

void
bs_error(struct bit_stream *s, char *msg, ...)
{
	va_list args;
	va_start(args, msg);
	bs_errorv(s, msg, args);
	va_end(args);
}

void
bs_errorv(struct bit_stream *s, char *msg, va_list args)
{
	if (s->err && msg)
		s->err(s, msg, args);
	if (s->err_jmp)
		longjmp(*s->err_jmp, 1);
}

void
bs_warn(struct bit_stream *s, char *msg, ...)
{
	va_list args;
	va_start(args, msg);
	bs_warnv(s, msg, args);
	va_end(args);
}

void
bs_warnv(struct bit_stream *s, char *msg, va_list args)
{
	if (s->warn && msg)
		s->warn(s, msg, args);
}

byte *
bs_get(struct bit_stream *s, uns len, char *msg, ...)
{
	if (unlikely(len >= 0x80000000 || (s->remains -= len) < 0)) {
		va_list args;
		va_start(args, msg);
		bs_errorv(s, msg, args);
		va_end(args);
		return NULL;
	}
	byte *p = s->pos;
	s->pos += len;
	return p;
}

byte *
bs_pop(struct bit_stream *s, uns len, char *msg, ...)
{
	if (unlikely(len >= 0x80000000 || (s->remains -= len) < 0)) {
		va_list args;
		va_start(args, msg);
		bs_errorv(s, msg, args);
		va_end(args);
		return NULL;
	}
	return s->pos;
}

int
bs_subrange(struct bit_stream *outer, struct bit_stream *inner, uns len, char *msg, ...)
{
	if (unlikely(len >= 0x80000000 || (outer->remains -= len) < 0)) {
		va_list args;
		va_start(args, msg);
		bs_error(outer, msg, args);
		va_end(args);
		return 0;
	}
	bzero(inner, sizeof(*inner));
	inner->pos = outer->pos;
	inner->remains = len;
	inner->err = outer->err;
	inner->warn = outer->warn;
	inner->priv = outer->priv;
	inner->err_jmp = outer->err_jmp;
	outer->pos += len;
	return 1;
}

unsigned int
bs_tell(struct bit_stream *s)
{
	return s->pos - s->start;
}

/*** BIT STREAMS ***/

void
bs_init_bits(struct bit_stream *s)
{
	s->rem = 0;
	s->part = 0;
}

unsigned int
bs_get_bits(struct bit_stream *s, unsigned int n)
{
	unsigned int x = 0;
	while (n) {
		if (!s->rem) {
			byte *p = bs_get(s, 1, "bs_get_bits: Out of packet");
			if (unlikely(!p))
				return 0;
			s->part = *p;
			s->rem = 8;
		}
		unsigned int c = (s->rem < n) ? s->rem : n;
		x = (x << c) | ((s->part >> (8 - s->rem)) & ((1 << c) - 1));
		n -= c;
		s->rem -= c;
	}
	return x;
}

unsigned int
bs_tell_bit_pos(struct bit_stream *s)
{
	return bs_tell(s)*8 - s->rem;
}
