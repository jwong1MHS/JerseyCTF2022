/*
 *	Parsing of ID3v2 Tags
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#ifndef _MP3_ID3_H
#define _MP3_ID3_H

#include "ucw/clists.h"
#include "mp3.h"

struct odes;
struct mempool;

/*** ID3v1 ***/

/* Parse a v1 tag to a Sherlock object */
struct odes *id3v1_parse(struct mp3_frame *fr, struct mempool *pool);

/*** ID3v2 ***/

/*
 *  Internal representation of ID3v2 tags. There can be multiple such tags in
 *  a single file (tag set), which override themselves in various ways, each tag
 *  consists of individual items (called frames in the specs, but we don't want
 *  to confuse them with the MP3 frames).
 */

// A single file can contain several ID3v2 tags, this structure puts them together
struct id3v2_tag_set {
	clist tags;
	struct mempool *pool;
};

struct id3v2_tag {
	cnode n;
	struct id3v2_tag_set *set;
	byte version, revision;		// Information extracted from tag header
	byte hdr_flags;
	clist items;			// A list of id3v2_item's (ID3 frames) in the tag
	struct bit_stream bs;		// Used during parsing, error handling cloned from mp3_frame->bs
};

struct id3v2_item {
	cnode n;
	struct id3v2_tag *tag;
	u32 id;
	uns length;
	uns flags;
	byte *data;
};

#define ID3_ID(a,b,c,d) (u32)((a<<24)|(b<<16)|(c<<8)|d)
#define ID3_NAME_CC(x) (((x)>>24)&255), (((x)>>16)&255), (((x)>>8)&255), ((x)&255)

struct id3v2_tag_set *id3v2_new_set(struct mempool *pool);
struct id3v2_tag *id3v2_parse(struct id3v2_tag_set *set, struct mp3_frame *fr);
void id3v2_dump(struct id3v2_tag_set *set);
void id3v2_dump_item(struct id3v2_item *it);

/*
 *  Tag items are stored in a zillion of different binary formats. These
 *  functions help to find and dissect them. All these functions assume that
 *  the error handling specified in id3v2_tag->bs doesn't return, i.e., that
 *  either the err_jmp is defined or the error function does an exit/longjmp
 *  itself.
 */
struct id3v2_item *id3v2_find_item(struct id3v2_tag_set *ts, u32 id);	// Find item and prepare for parsing
void id3v2_seek_item(struct id3v2_item *it);				// Prepare for parsing of a given item

byte *id3v2_parse_string(struct id3v2_item *it, int encoding);		// encoding == -1 if specified at start of the string
byte *id3v2_parse_fixed_string(struct id3v2_item *it, int len);
uns id3v2_parse_byte(struct id3v2_item *it);

/*
 *  The high-level interface: Return a Sherlock object containing all
 *  tag items as its subobjects, resolving precedence of tags and ignoring
 *  unknown and unparseable tags.
 */
struct odes *id3v2_item_to_obj(struct id3v2_item *it);
struct odes *id3v2_set_to_obj(struct id3v2_tag_set *set);
struct odes *id3v2_find_item_obj(struct odes *o, u32 id);

/*** Stuff common for both v1 and v2 tags ***/

byte *id3_genre_tab(uns id);

#endif
