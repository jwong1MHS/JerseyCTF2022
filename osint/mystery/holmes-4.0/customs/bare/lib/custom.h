/*
 *	Sherlock: Custom Parts of Configuration
 *
 *	(c) 2001--2004 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

/*
 *	This is a "bare" customization -- the only things which are defined
 *	are those that are absolutely necessary. You'll find examples of what
 *	is possible in free/lib/custom.h.
 */

/* Versions */

#define SHERLOCK_VERSION_SUFFIX "-bare"	/* String appended to version number to identify custom versions */
#define CUSTOM_INDEX_TYPE 'B'		/* A byte identifying our flavor of index format [please change!] */
#define CUSTOM_INDEX_VERSION 0x01	/* Remember to increase after each change of custom parts of the index */

/* Structures we'll need in function parameters */

struct card_attr;
struct odes;

/* Word types (at most 8 of them + WT_MAX) */

#define WT_TEXT			0	/* Ordinary text */
#define WT_MAX			1

/* Descriptive names used for user output */
#define WORD_TYPE_USER_NAMES							\
   "text", "type1", "type2", "type3", "type4", "type5", "type6", "type7"

/* Keywords for word type names */
#define WORD_TYPE_NAMES				\
	T(TEXT, 1 << WT_TEXT)

/*
 * These types are always matched with/without accents if accent mode is set to "auto",
 * regardless of accentedness of the current document.
 */
#define WORD_TYPES_AUTO_ACCENT_ALWAYS_STRICT 0
#define WORD_TYPES_AUTO_ACCENT_ALWAYS_STRIP 0

/* These types belong to all languages */
#define WORD_TYPES_ALL_LANGS 0

/* These types don't belong to any language, so neither language matching nor lemmatization affects them */
#define WORD_TYPES_NO_LANG 0

/* Meta information types (at most 16 of them + MT_MAX) */

#define MT_MAX			0

#define META_TYPE_USER_NAMES							\
   "meta0", "meta1", "meta2", "meta3", "meta4", "meta5", "meta6", "meta7",	\
   "meta8", "meta9", "meta10", "meta11", "meta12", "meta13", "meta14", "meta15"

/* Keywords for meta type names */
#define META_TYPE_NAMES

#define META_TYPES_AUTO_ACCENT_ALWAYS_STRICT 0
#define META_TYPES_AUTO_ACCENT_ALWAYS_STRIP 0
#define META_TYPES_ALL_LANGS 0
#define META_TYPES_NO_LANG 0

/* String types (at most 8 of them + ST_MAX) */

#define ST_MAX			0

#define STRING_TYPE_USER_NAMES							\
   "type0", "type1", "type2", "type3", "type4", "type5", "type6", "type7"

#define STRING_TYPE_NAMES

#define STRING_TYPES_URL 0
/* These must be indexed in lowercase form */
#define STRING_TYPES_CASE_INSENSITIVE 0

static inline void
custom_index_strings(struct odes *o UNUSED, void (*f)(char *text, uns type) UNUSED)
{
  /*
   * Call f for all strings you want to add to the index.
   * ST_URL, ST_HOST, ST_DOMAIN and ST_REF are indexed automatically
   * if they are defined.
   */
}

/*
 *  Definitions of custom attributes:
 *
 *  First of all, you need to define your own card_attr fields which will
 *  contain your attributes: CUSTOM_CARD_ATTRS lists them.
 *  Please order the attributes by decreasing size to get optimum padding.
 *
 *  Then define custom_create_attrs() which will get the object description
 *  and set your card_attr fields accordingly.
 *
 *  Finally, you have to define CUSTOM_ATTRS with matching rules:
 *
 *  INT_ATTR(id, keyword, get_func, parse_func) -- unsigned integer attribute
 *
 *  id		C identifier of the attribute
 *  keywd	search server keyword for the attribute
 *  int get_func(struct card_attr *ca)
 *		get attribute value from the card_attr
 *  char *parse_func(u32 *dest, char *value, uns intval)
 *		parse value in query (returns error message or NULL)
 *		for KEYWD = "string", it gets value="string", intval=0
 *		for KEYWD = num, it gets value=NULL, intval=num.
 *
 *  SMALL_SET_ATTR(id, keyword, get_func, parse_func)
 *    -- integers 0..31 with set matching
 *
 *  A good place for definitions of the functions is lib/custom.c.
 */

/* No custom attributes defined yet */

#define CUSTOM_CARD_ATTRS
#define CUSTOM_ATTRS
static inline void custom_create_attrs(struct odes *odes UNUSED, struct card_attr *ca UNUSED) { }

