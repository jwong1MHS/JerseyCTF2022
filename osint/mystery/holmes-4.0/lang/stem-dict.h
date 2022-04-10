/*
 *  Simple Suffix Dictionary Driven Stemmer: Dictionary Format
 *
 *  (c) 2005 Martin Mares <mj@ucw.cz>
 */

#define STEM_DICT_MAGIC 0x6454531e
#define MAX_PREFIXES 3
#define MAX_PREFIX_LEN 8

struct stem_dict_hdr {
  u32 magic;				/* STEM_DICT_MAGIC */
  byte charset[32];
  byte prefixes[MAX_PREFIXES][MAX_PREFIX_LEN];
  byte prefix_lengths[MAX_PREFIX_LEN];
  u32 num_prefixes;
  u32 suffix_table_start;		/* -> u32 suffix_start[] -> byte reverse_suffix[] */
  u32 num_suffixes;
  u32 max_suffix;
  u32 pattern_table_start;		/* -> u32 pattern_start[] -> struct stem_dict_pattern */
  u32 num_patterns;
  u32 stem_tree_start;
  u32 stem_tree_length;
};

typedef u16 suffix_id_t;
#define MAX_SUFFIXES 32767
#define SUFFIX_FLAG_LEMMA 0x8000

typedef u16 pattern_id_t;
#define MAX_PATTERNS 65535
#define GET_PATTERN_ID(p) GET_U16(p)

struct stem_dict_pattern {
  u16 pattern_flags;			/* bottom 8 bits are the prefix mask */
  suffix_id_t suffixes[0];		/* suffix numbers, possibly with SUFFIX_FLAG_LEMMA */
} PACKED;

#define PATTERN_FLAG_NOACCENT 0x8000

/*
 *  The stem tree is a sequence of opcodes of the following form:
 *
 *	00000000  br-large	large branching: u8 entries; { u8 char; u32 offset; }
 *	00llllll  br-small	small branching with <l> entries of type { u8 char; u16 offset; }
 *	01llllll  leaf		leaf (stem) with <l> characters; followed by { pattern_id_t pattern; }
 *	10xxxxxx  sub-ptr	pointer to a substem (3 more bytes follow)
 *	11xxxxxx  main-ptr	pointer from substem to the main stem (3 more bytes follow)
 *	1110iiii  var-id	lemma variant ID
 *	11110000  skip-char	skip next character { u8 char; }
 *	11110001  repeat	repeat the same stem with a new pattern
 *	1111xxxx  RFU		reserved for future use
 *
 *  The grammar of stem tree nodes:
 *
 *	node = skip-char* (br-large | br-small)? stem*
 *	stem = leaf stem-data (repeat stem-data)*
 *	stem-data = (var-id? main-ptr? | sub-ptr+)
 */

#define MAX_BRANCHING 128
#define MAX_STEM_LEN 63
