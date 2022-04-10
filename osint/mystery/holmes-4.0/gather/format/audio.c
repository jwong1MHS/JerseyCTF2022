/*
 *	Audio - Common Routines for MP3 and OGG Parsers
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "sherlock/object.h"
#include "ucw/unicode.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-unicode.h"
#include "ucw/conf.h"
#include "ucw/string.h"
#include "charset/unicat.h"
#include "gather/gather.h"
#include "gather/format/audio.h"

#include <string.h>

struct odes *audio_obj;
struct audio_value audio_len;
struct audio_value audio_bitrate;
struct audio_value audio_srate;
struct audio_value audio_channels;
static int audio_size;

static struct fastbuf *text_out, *meta_out, *attr_out, *out;
static uns word_type;
static uns word_buf[MAX_WORD_CHARS];
static uns word_len;
static uns word_start;

static void
flush_word(void)
{
  if (!word_len || word_len >= MAX_WORD_CHARS) /* Enormous words are ignored instead of truncated */
    return;
  if (!word_start)
    bputc(out, ' ');
  else
    {
      word_start = 0;
      if (out != attr_out)
        bputc(out, 0x90 + word_type); /* WT/MT start */
    }
  for (uns i = 0; i < word_len; i++)
    bput_utf8(out, word_buf[i]);
  word_len = 0;
}

static void
add_char(uns c)
{
  if (Uprint(c) && !Uspace(c))
    {
      if (word_len < MAX_WORD_CHARS)
        word_buf[word_len++] = c;
    }
  else
    flush_word();
}

static void
add_string(byte *s)
{
  word_start = 1;
  while (*s)
    {
      uns u;
      s = utf8_get(s, &u);
      add_char(u);
    }
  flush_word();
}

void
audio_add_text(uns wt, byte *s)
{
  out = text_out;
  word_type = wt;
  add_string(s);
}

void
audio_add_meta(uns mt, byte *s)
{
  out = meta_out;
  word_type = mt;
  add_string(s);
}

void
audio_add_attr(uns attr, byte *s)
{
  fbgrow_reset(attr_out);
  out = attr_out;
  add_string(s);
  uns l = btell(attr_out);
  fbgrow_rewind(attr_out);
#define MAX_LEN 2500
  byte buf[MAX_LEN + 1];
  if (l < MAX_LEN)
    {
      bread(attr_out, buf, l);
      buf[l] = 0;
    }
  else /* Too long attribute, truncate for the indexer */
    {
      bread(attr_out, buf, MAX_LEN);
      byte *p = buf + MAX_LEN;
      while (p-- > buf && *p != ' ');
      *p = 0;
    }
  if (*buf)
    obj_add_attr(audio_obj, attr, buf);
}

static void
audio_hash(void)
{
  gobj_calc_sum();
  if (!gthis->MD5_valid)
    return;

  char sum[MD5_HEX_SIZE];
  mem_to_hex(sum, gthis->MD5, MD5_SIZE, MEM_TO_HEX_UPCASE);
  obj_set_attr(audio_obj, 'h', sum);
}

void
audio_start(void)
{
  audio_obj = obj_new(gthis->pool);
  attr_out = fbgrow_create(1024);
  text_out = fbmem_create(1024);
  meta_out = fbmem_create(1024);
  audio_len = audio_bitrate = audio_srate = audio_channels = (struct audio_value) { -1, -1 };
  obj_set_attr(audio_obj, 'f', "a");
}

void
audio_end(void)
{
  audio_size = !gthis->truncated ? (int)gthis->orig_size : gthis->expected_size ? (int)gthis->expected_size : -1;
  if (audio_size >= 0)
    obj_set_attr_num(audio_obj, 's', audio_size);
  if (audio_len.value >= 0)
    obj_set_attr_num(audio_obj, 'l', audio_len.value);
  if (audio_bitrate.value >= 0)
    obj_set_attr_num(audio_obj, 'b', audio_bitrate.value);
  if (audio_srate.value >= 0)
    obj_set_attr_num(audio_obj, 'r', audio_srate.value);
  if (audio_channels.value >= 0)
    obj_set_attr_num(audio_obj, 'c', audio_channels.value);
  audio_hash();
  obj_add_son_ref(gthis->aa, 'f' + OBJ_ATTR_SON, audio_obj);
  audio_cancel();
}

void
audio_cancel(void)
{
  bclose(attr_out);
  if (btell(text_out))
    gthis->text = text_out;
  else
    bclose(text_out);
  if (btell(meta_out))
    gthis->meta = meta_out;
  else
    bclose(meta_out);
}

void
audio_set_value(struct audio_value *type, int value, int prior)
{
  if (value >= 0 && prior > type->prior)
    {
      type->value = value;
      type->prior = prior;
    }
}

