/*
 *	Sherlock: Data structures used in indices
 *
 *	(c) 2001--2008 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#ifndef _SHERLOCK_INDEX_H
#define _SHERLOCK_INDEX_H

enum custom_op {	/* Passed to custom parsers and also used internally in search/parse.y */
  CUSTOM_OP_LT,
  CUSTOM_OP_GT,
  CUSTOM_OP_LE,
  CUSTOM_OP_GE,
  CUSTOM_OP_EQ,
  CUSTOM_OP_NE
};

#include "custom/lib/custom.h"

/*
 *  Magic number which should help to avoid mixing incompatible indices.
 *  Syntax: <version><revision><custom-type><custom-version>
 *  Remember to increase with each change of index format.
 */

#define INDEX_VERSION (0x3a020000|((CUSTOM_INDEX_TYPE)<< 8)|(CUSTOM_INDEX_VERSION))

/* Current version of bucket format */

#define BUCKET_VERSION "2"

/*
 *  Aside from parameters set in cf/indexer, indexed words are limited by two constants:
 *
 *  MAX_WORD_CHARS is the maximum number of Unicode characters a word can have.
 *
 *  MAX_WORD_BYTES is the maximum number of bytes in the UTF-8 encoding of a word.
 *  It is assumed that any word which has entered the index (i.e., it has passed
 *  the MAX_WORD_CHARS limit) fits in this number of bytes.
 *
 *  These constants are currently defined in sherlock/default.cfg, because they are used
 *  not only by Sherlock, but also by liblang.
 *
 *  Hash tables used by the indexer also require that MAX_WORD_BYTES is a multiple of 4.
 */

/* Word and string types are defined in custom/lib/custom.h */

/* Types used for storing contexts */

#ifdef CONFIG_CONTEXTS
#if CONFIG_MAX_CONTEXTS == 32768
typedef u16 context_t;
#define bget_context bgetw
#define bput_context bputw
#define GET_CONTEXT GET_U16
#define PUT_CONTEXT PUT_U16
#elif CONFIG_MAX_CONTEXTS == 256
typedef byte context_t;
#define bget_context bgetc
#define bput_context bputc
#define GET_CONTEXT GET_U8
#define PUT_CONTEXT PUT_U8
#else
#error CONFIG_MAX_CONTEXTS set to an invalid value.
#endif
#else
struct fastbuf;
typedef struct { } context_t;
static inline uns bget_context(struct fastbuf *b UNUSED) { return 0; }
static inline void bput_context(struct fastbuf *b UNUSED, uns context UNUSED) { }
#define GET_CONTEXT(p) 0
#define PUT_CONTEXT(p,x) do {} while(0)
#endif

/* Merging hashes */

#ifdef CONFIG_MERGING_HASHES
#ifndef SHERLOCK_MERGING_HASH_SIZE
#define SHERLOCK_MERGING_HASH_SIZE 4
#endif
#else
#define SHERLOCK_MERGING_HASH_SIZE 0
#endif

/* Index card attributes */

struct card_attr {
  u32 card;				/* Reference to card description (either oid or filepos) */
#ifdef CONFIG_SITES
  u32 site_id;
#endif
  byte merging_hash[SHERLOCK_MERGING_HASH_SIZE];
  area_t area;
  CUSTOM_CARD_ATTRS			/* Include all custom attributes */
  byte weight;
  byte flags;
#ifdef CONFIG_LASTMOD
  byte age;				/* Document age in pseudo-logarithmic units wrt. reference time */
#endif
#ifdef CONFIG_FILETYPE
  byte type_flags;			/* File type flags (see below) */
#endif
};

enum card_flag {
  CARD_FLAG_EMPTY = 1,			/* Empty document (redirect, robot file etc.) [scanner] */
  CARD_FLAG_ACCENTED = 2,		/* Document contains accented characters [scanner] */
  CARD_FLAG_DUP = 4,			/* Removed as a duplicate [merger] */
					/* Overriden by another index [sherlockd] */
  CARD_FLAG_MERGED = 8,			/* Destination of a merge [merger] */
  CARD_FLAG_FRAMESET = 16,		/* Contains a frameset to be ignored [scanner] */
  CARD_FLAG_CUSTOM1 = 32,		/* Three custom flags (can be used by customization) */
  CARD_FLAG_CUSTOM2 = 64,
  CARD_FLAG_CUSTOM3 = 128,
};

#ifndef CARD_POS_SHIFT			/* (can be overriden in custom.h) */
#define CARD_POS_SHIFT 5		/* Card positions are shifted this # of bits to the right */
#endif

/*
 *  We store document type and several other properties in card_attr->type_flags.
 *  Here we define only the basic structure, the details are defined in custom.h
 *  (the list of type names custom_file_type_names[] and also setting of the file
 *  types in custom_create_attrs()).
 *
 *  bits 7--4	file type: (0-7: text types, 8-15: other types, defined by custom.h)
 *  bits 3--0	type-dependent information, for text types it's document language code
 */

#ifdef CONFIG_FILETYPE
#define CA_GET_FILE_TYPE(a) ((a)->type_flags >> 4)
#define CA_GET_FILE_INFO(a) ((a)->type_flags & 0x0f)
#define CA_GET_FILE_LANG(a) ((a)->type_flags & 0x80 ? 0 : CA_GET_FILE_INFO(a))
#define MAX_FILE_TYPES 16
#define FILETYPE_IS_TEXT(f) ((f) < 8)
char *ext_ft_parse(u32 *dest, char *value, uns intval);
extern char *custom_file_type_names[MAX_FILE_TYPES];
#define FILETYPE_STAT_VARS uns matching_per_type[MAX_FILE_TYPES];
#define FILETYPE_SHOW_STATS(q,s,f) ext_ft_show(s,f)
#define FILETYPE_INIT_STATS(q,s) bzero(s->matching_per_type, sizeof(s->matching_per_type))
#define FILETYPE_MERGE_STATS(q,f,t) for (uns i=0; i<MAX_FILE_TYPES; i++) t->matching_per_type[i] += f->matching_per_type[i]
#ifdef CONFIG_COUNT_ALL_FILETYPES
#define FILETYPE_ATTRS LATE_SMALL_SET_ATTR(ftype, FILETYPE, CA_GET_FILE_TYPE, ext_ft_parse)
#define FILETYPE_EARLY_STATS(q,s,a) s->matching_per_type[CA_GET_FILE_TYPE(a)]++
#define FILETYPE_LATE_STATS(q,s,a)
#else
#define FILETYPE_ATTRS SMALL_SET_ATTR(ftype, FILETYPE, CA_GET_FILE_TYPE, ext_ft_parse)
#define FILETYPE_EARLY_STATS(q,s,a)
#define FILETYPE_LATE_STATS(q,s,a) s->matching_per_type[CA_GET_FILE_TYPE(a)]++
#endif
#else
#define FILETYPE_ATTRS
#define FILETYPE_STAT_VARS
#define FILETYPE_INIT_STATS(q,s)
#define FILETYPE_EARLY_STATS(q,s,a)
#define FILETYPE_LATE_STATS(q,s,a)
#define FILETYPE_SHOW_STATS(q,s,f)
#define FILETYPE_MERGE_STATS(q,f,t)
#endif

#ifdef CONFIG_LANG
/* You can use language matching without CONFIG_FILETYPE, but you have to define CA_GET_FILE_LANG yourself. */
#define LANG_ATTRS SMALL_SET_ATTR(lang, LANG, CA_GET_FILE_LANG, ext_lang_parse)
char *ext_lang_parse(u32 *dest, char *value, uns intval);
#else
#define LANG_ATTRS
#endif

#ifdef CONFIG_AREAS
#define CA_GET_AREA(a) ((a)->area)
#define SPLIT_ATTRS INT_ATTR(area, AREA, CA_GET_AREA, ext_area_parse)
char *ext_area_parse(u32 *dest, char *value, uns intval);
#else
#define SPLIT_ATTRS
#endif

/*
 * A list of all extended attributes: custom attributes and also some
 * built-in attributes treated in the same way.
 */

#define EXTENDED_ATTRS CUSTOM_ATTRS FILETYPE_ATTRS LANG_ATTRS SPLIT_ATTRS

/*
 * A list of all statistics collectors, also composed of custom parts
 * and built-in parts.
 */

#ifndef CUSTOM_STAT_VARS
#define CUSTOM_STAT_VARS
#define CUSTOM_INIT_STATS(q,s)
#define CUSTOM_EARLY_STATS(q,s,a)
#define CUSTOM_LATE_STATS(q,s,a)
#define CUSTOM_SHOW_STATS(q,s,f)
#define CUSTOM_MERGE_STATS(q,f,t)
#endif

#define EXTENDED_STAT_VARS CUSTOM_STAT_VARS FILETYPE_STAT_VARS
#define EXTENDED_INIT_STATS(q,s) CUSTOM_INIT_STATS(q,s) FILETYPE_INIT_STATS(q,s)
#define EXTENDED_EARLY_STATS(q,s,a) CUSTOM_EARLY_STATS(q,s,a) FILETYPE_EARLY_STATS(q,s,a)
#define EXTENDED_LATE_STATS(q,s,a) CUSTOM_LATE_STATS(q,s,a) FILETYPE_LATE_STATS(q,s,a)
#define EXTENDED_SHOW_STATS(q,s,f) CUSTOM_SHOW_STATS(q,s,f) FILETYPE_SHOW_STATS(q,s,f)
#define EXTENDED_MERGE_STATS(q,f,t) CUSTOM_MERGE_STATS(q,f,t) FILETYPE_MERGE_STATS(q,f,t)

/*
 * Define empty custom matchers, if the customization doesn't specify them.
 */
#ifndef CUSTOM_MATCH_VARS
#define CUSTOM_MATCH_VARS
#endif
#ifndef CUSTOM_MATCH_INIT
#define CUSTOM_MATCH_INIT(q)
#endif
#ifndef CUSTOM_MATCH_PARSE
#define CUSTOM_MATCH_PARSE
#endif
#ifndef CUSTOM_MATCH_CACHE_KEY
#define CUSTOM_MATCH_CACHE_KEY(q,mp) NULL
#endif
#ifndef CUSTOM_MATCH_SHOW
#define CUSTOM_MATCH_SHOW(q,ca,add)
#endif

/*
 * Define empty per-thread storage, if the customization doesn't specify it.
 */
#ifndef CUSTOM_REF_VARS
#define CUSTOM_REF_VARS
#endif
#ifndef CUSTOM_REF_INIT
#define CUSTOM_REF_INIT(c) do {} while(0)
#endif
#ifndef CUSTOM_REF_CLEANUP
#define CUSTOM_REF_CLEANUP(c) do {} while(0)
#endif

/* String fingerprints */

struct fingerprint {
  byte hash[12];
};

void fingerprint(byte *string, struct fingerprint *fp);

static inline u32
fp_hash(struct fingerprint *fp)
{
  return (fp->hash[0] << 24) | (fp->hash[1] << 16) | (fp->hash[2] << 8) | fp->hash[3];
}

/* The card fingerprints */

struct card_print {
  struct fingerprint fp;
  u32 cardid;
};

/* URL keys */

#define URL_KEY_BUF_SIZE (3*MAX_URL_SIZE)
byte *url_key(byte *url, byte *buf);
void url_fingerprint(byte *url, struct fingerprint *fp);
void url_key_init(void);

/* Conversion of document age from seconds to our internal units */

static inline int
convert_age(ucw_time_t lastmod, ucw_time_t reftime)
{
  ucw_time_t age;
  if (reftime < lastmod)		/* past times */
    return -1;
  age = (reftime - lastmod) / 3600;
  if (age < 48)				/* last 2 days: 1 hour resolution */
    return age;
  age = (age-48) / 24;
  if (age < 64)				/* next 64 days: 1 day resolution */
    return 48 + age;
  age = (age-64) / 7;
  if (age < 135)			/* next 135 weeks: 1 week resolution */
    return 112 + age;
  age = (age-135) / 52;
  if (age < 8)				/* next 8 years: 1 year resolution */
    return 247 + age;
  return 255;				/* then just "infinite future" */
}

/* Reference chains */

#ifdef CONFIG_32BIT_REFERENCES
#define HARD_META_LIMIT	((1<<19) - 1)
#define HARD_WORD_LIMIT	((1<<23) - 1)
typedef u32 ref_pos_t;
#define POS_FIRST_META (1U<<31)		/* The search server encodes meta flag and type to ref_pos_t internally */
#define POS_META_SHIFT 24
#define POS_NOWHERE ~0U
#else
#define HARD_META_LIMIT	((1<<11) - 1)
#define HARD_WORD_LIMIT	((1<<15) - 1)
typedef u16 ref_pos_t;
#define POS_FIRST_META (1<<15)
#define POS_META_SHIFT 11
#define POS_NOWHERE 0xffff
#endif

#define HARD_MAX_SLICES 8		/* At most 8, limited by bit masks in refchain format */

#endif
