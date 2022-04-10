/*
 *	Sherlock WML Parser
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_FORMAT_WML_WML_H
#define _SHERLOCK_GATHER_FORMAT_WML_WML_H

struct wml_tag {
  char *name;
  unsigned int type;
  unsigned int flags;
};

const struct wml_tag *is_wml_tag(const char *, unsigned int);

enum wml_tag_id {
  T_IGNORE = 0,
  T_WML,
  T_CARD,
  T_IMG,
  T_A,
  T_ANCHOR,
  T_GO,
  T_POSTFIELD,
  T_META,
  T_TITLED_ELEMENT,
  T_TIMER,
  T_MASK = 0xfffffff,
  T_FLOW = 0x10000000,		/* This tag doesn't break text flow */
  T_HARD = 0x20000000,		/* This tag separates sentences */
};

enum wml_tag_flag {
  F_EM = 0x1,
  F_STRONG = 0x2,
  F_I = 0x4,
  F_B = 0x8,
  F_U = 0x10,
  F_BIG = 0x20,
  F_SMALL = 0x40,
  F_CARD = 0x80,
  F_TEMPLATE = 0x100,
};

#endif
