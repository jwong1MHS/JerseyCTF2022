/*
 *	Sherlock HTML Parser
 *
 *	(c) 1997--2001 Martin Mares <mj@ucw.cz>
 */

struct entity {
  char *name;
  unsigned int value;
};

const struct entity *is_entity(const char *, unsigned int);

#define X2(x,y) (((y) << 8) | (x))
#define X3(x,y,z) (((z) << 16) | ((y) << 8) | (x))

struct tag {
  char *name;
  unsigned int type;
  unsigned int arg;
};

const struct tag *is_tag(const char *, unsigned int);

enum tag_id {
  T_IGNORE = 0,
  T_ON,
  T_OFF,
  T_BASE,
  T_META,
  T_A,
  T_IMG,
  T_APPLET,
  T_FORM,
  T_LINK,
  T_FRAME,
  T_IFRAME,
  T_BLOCKQUOTE,
  T_AREA,
  T_INSDEL,
  T_TABLE,
  T_OBJECT,
  T_PARAM,
  T_EMBED,
  T_FONT,
  T_BASEFONT,
  T_SCRIPT,
  T_HTML,
  T_BOTH = 0x08000000,			/* This tag is permitted in both head and body */
  T_FLOW = 0x10000000,			/* This tag doesn't break text flow */
  T_HARD = 0x20000000,			/* This tag separates sentences */
  T_HEAD = 0x40000000,			/* This tag is permitted in document head */
};

enum attrib {
  A_EM = 1,
  A_H1 = 2,
  A_H2 = 4,
  A_H3 = 8,
  A_H4 = 0x10,
  A_H5 = 0x20,
  A_H6 = 0x40,
  A_TT = 0x80,
  A_B = 0x100,
  A_I = 0x200,
  A_TITLE = 0x400,
  A_OBJECT = 0x800,
  A_OBJECT_FLASH = 0x1000,
  A_STRONG = 0x2000,
  A_SUP = 0x4000,
  A_SUB = 0x8000,
  A_CODE = 0x10000,
  A_DFN = 0x20000,
  A_SAMP = 0x40000,
  A_KBD = 0x80000,
  A_VAR = 0x100000,
  A_ADDRESS = 0x200000,
  A_STRIKE = 0x400000,
  A_BIG = 0x800000,
  A_SMALL = 0x1000000,
  A_ACRONYM = 0x2000000,
  A_SMALLFONT = 0x4000000,
  A_LARGEFONT = 0x8000000
};

/* These attribute flags are automatically cleared when T_HARD is encountered */
#define ATTRIB_CLEAR_ON_T_HARD (~0U)

/* These attribute flags are automatically cleared outside T_HEAD */
#define ATTRIB_CLEAR_IN_BODY A_TITLE

/* Attributes with these flags can occur only once in a document */
#define ATTRIB_ONLY_ONCE A_TITLE
