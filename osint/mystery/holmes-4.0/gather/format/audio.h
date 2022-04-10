/*
 *	Audio - Common Definitions for MP3 and OGG Parsers
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_FORMAT_AUDIO_H
#define _SHERLOCK_GATHER_FORMAT_AUDIO_H

struct odes;

struct audio_value {
  int value;
  int prior;
};

extern struct odes *audio_obj;				/* '(f' attribute */
extern struct audio_value audio_len;			/* Length in seconds and how much we can trust it */
extern struct audio_value audio_bitrate;		/* Bits per second */
extern struct audio_value audio_srate;			/* Sample rate in Hz */
extern struct audio_value audio_channels;		/* Number of channel */

void audio_start(void);
void audio_end(void);
void audio_cancel(void);
void audio_add_text(uns wt, byte *s);
void audio_add_meta(uns mt, byte *s);
void audio_add_attr(uns a, byte *s);
void audio_set_value(struct audio_value *type, int value, int prior);

#endif
