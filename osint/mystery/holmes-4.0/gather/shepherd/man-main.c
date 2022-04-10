/*
 *	Sherlock Shepherd -- Manual Control -- Main Routines
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/conf.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/man.h"

#include <time.h>

struct mempool *man_pool;
time_t man_ref_time;

void (*man_usage_callback)(byte *msg);

struct man_options man_opt, man_opt_defaults = {
  .server = "localhost",
  .set_weight = -1,
  .set_freq = -1,
  .set_section = -1,
#ifdef CONFIG_AREAS
  .set_area_soft = -1,
  .set_area_hard = -1,
  .set_area_plan = -1,
#endif
  .hist_num_boxes = 60,
  .hist_box_width = 60,
  .age_display_unit = 1,
};

void
man_init(void)
{
  static uns done;
  if (done)
    return;
  done++;
  man_pool = mp_new(8192);
  man_ref_time = time(NULL);
  man_opt_defaults.set_area = AREA_ANY;
  man_reset();
}

void
man_reset(void)
{
  mp_flush(man_pool);
  man_opt = man_opt_defaults;
  site_hash_reset();
}

uns
man_raw_url_age(struct url_state *s)
{
  return (s->last_seen > (u32) man_ref_time) ? 0 : (man_ref_time - s->last_seen);
}

uns
man_url_age(struct url_state *s)
{
  if (man_opt.show_lastmod)
    return s->last_seen;
  else
    {
      uns age = man_raw_url_age(s);
      if (man_opt.multi_age && s->refresh_freq)
	age *= s->refresh_freq;
      return age;
    }
}

void
man_usage(byte *msg, ...)
{
  va_list args;

  va_start(args, msg);
  byte *p = mp_vprintf(man_pool, msg, args);
  if (man_usage_callback)
    {
      man_usage_callback(p);
      ASSERT(0);
    }
  else
    die("%s", p);
  va_end(args);
}

static struct cf_section shepman_config = {
  CF_ITEMS {
    CF_UNS("HistNumBoxes", &man_opt_defaults.hist_num_boxes),
    CF_UNS("HistBoxWidth", &man_opt_defaults.hist_box_width),
    CF_UNS("AgeDisplayUnit", &man_opt_defaults.age_display_unit),
    CF_UNS("BinaryForwardLimit", &man_binary_forward_limit),
    CF_UNS("BinaryFirstSkip", &man_binary_first_skip),
    CF_UNS("BinarySplitLimit", &man_binary_split_limit),
    CF_UNS("TextBlockLimit", &man_text_block_limit),
  CF_END
  }
};

static void CONSTRUCTOR
read_config(void)
{
  cf_declare_section("ShepMan", &shepman_config, 0);
}
