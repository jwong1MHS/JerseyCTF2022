/*
 *	Sherlock Gatherer
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_GATHER_H
#define _SHERLOCK_GATHER_GATHER_H

#include "ucw/clists.h"
#include "ucw/fastbuf.h"
#include "ucw/url.h"
#include "ucw/md5.h"
#include "sherlock/object.h"

/* ginit.c */

void gatherer_init(void);		/* Call before using the library */

/* gconfig.c */

extern uns max_obj_size;		/* Handled separately by all downloaders */
extern uns allow_truncate;		/* Allow truncated objects */
extern uns max_decode_size;		/* Handled separately by all decoders */
extern uns trace_decode;		/* Handled separately by all decoders */
extern uns log_ref_errors, log_base_errors;
extern uns min_summed_size;		/* Minimum size allowed for checksum calculation */
extern clist gaccess_list;		/* IP address access list */
extern int min_ims_delay;		/* If-mod-since: require document new at least by this time, -1=no IMS */
extern uns max_refresh_age;		/* Force full reload after this time */
extern char *gather_filter_name;	/* Name of the main filter */
extern uns gather_min_compression;	/* Objects that cannot be compressed well enough, are stored uncompressed */
extern uns trace_resolve;		/* Trace IP address and SKEY resolving */
extern uns max_parser_alloc;		/* Maximum amount of memory allocated by parser_malloc() et al. */

/* gobject.c */

struct gobj_ref {
  cnode n;
  int type;
  int id;
  int dont_follow;
  byte *content_type;			/* Probable content type */
  byte url[1];
};

struct gobject {
  /* Each document being gathered is represented by this structure */
  struct mempool *pool;			/* Everything is allocated from this pool */
  byte *url;				/* URL of the object */
  struct url url_s;
  byte *base_url;			/* URL everything else is relative to */
  struct url base_url_s;
  char *file_name;			/* if != NULL, use this instead of url_s.rest when guessing content-type */
  ucw_time_t start_time;			/* When we started processing the request */
  uns start_time_us;			/* Microsecond part of start_time */
  ucw_time_t lastmod_time;
  ucw_time_t expires_time;
  ucw_time_t if_modified_since_time;	/* Download only if modified since this time */
  byte *content_encoding;
  byte *content_type;
  byte *charset;
  byte *language;
  byte *http_server;			/* HTTP "Server:" header */
  byte *etag;				/* HTTP entity tag */
  byte *if_different_etag;		/* Download only if etag differs */
  unsigned int orig_size;		/* Size of original file */
  int expected_size;			/* Expected size of original file if truncated */
  byte truncated;
  unsigned int conv_count;		/* Conversion counter */
  byte MD5[MD5_SIZE];			/* MD5 sum of document contents */
  int MD5_valid;
  clist ref_list;
  uns ref_count;
  uns dont_follow_links;		/* Don't follow any links */
  uns dont_save_contents;		/* Don't store document contents */
  byte *auth_user;			/* Authentication */
  byte *auth_pass;
  struct fastbuf *contents;		/* Stream with object contents */
  struct fastbuf *text;			/* Stream with extracted text */
  struct fastbuf *meta;			/* Stream with extracted meta information */
  struct fastbuf *thumbnail;		/* Stream with image thumbnail or some other binary attachment */
  struct fastbuf *temp;			/* Temporary output stream */
  struct odes *aa;			/* Additional attributes */
  int error_code;			/* Error code of last error and its error message */
  byte *error_msg;
  void (*error_hook)(void);		/* Called by gerror() to finish the process */
  struct odes *refreshing;		/* Old object we're trying to refresh */
  int robot_file_p;			/* We're currently processing a robots.txt file */
  uns parser_malloced;			/* Total memory allocated by parser_malloc() and friends */
  uns filter_error_code;		/* Error code returned by the filter */
  byte *filter_language;		/* Language as presented to the filter */
  int filter_user_mark;			/* User-defined filtering mark */
};

struct gobject *gobj_new(struct mempool *);
void gobj_free(struct gobject *);

extern struct gobject *gthis;
void gerror(int code, char *msg, ...) NONRET FORMAT_CHECK(printf,2,3);
void gobj_set_redirect(char *msg, ...) FORMAT_CHECK(printf,1,2);
byte *gstrdup(byte *str);
struct gobj_ref *gobj_add_ref(int type, byte *url);
struct gobj_ref *gobj_add_ref_full(int type, byte *url, byte *ctype, struct url *base_url);
byte *gobj_parse_url(struct url *url, byte *u, byte *msg, uns allow_rel);
struct url *gobj_base_url(void);
void gobj_calc_sum(void);
void gobj_truncate(void);

/* diff.c */

uns gobj_check_update(void);

enum gobj_diff {			/* Returned by gobj_check_update() to report what has changed */
  GOBJ_CHG_TEXT_SMALL = 1,		/* Minor changes to text */
  GOBJ_CHG_TEXT_LARGE = 2,		/* Major changes to text */
  GOBJ_CHG_REFS = 4,			/* Outside links changed */
  GOBJ_CHG_HTTP = 8,			/* HTTP Last Modified or ETag changed */
  GOBJ_CHG_FORCED = 16,			/* MaxRefreshAge exceeded */
  GOBJ_CHG_REDIRECT = 32,		/* Redirect changed */
  GOBJ_CHG_BRAND_NEW = 65536		/* No previous version known */
  /* Remember to update diffs[] in gtest.c */
};

/* bucket.c */

uns gobj_write(struct fastbuf *b, uns bucket_type, uns flags);

enum gobj_write_flags {			/* What to write; general attributes are always included */
  GWF_DUMP_BODY = 1,			/* Dump document body */
  GWF_DUMP_SOURCE = 2,			/* Dump document source */
};

/* gfilter.c */

void gather_init_filter(void);
void gather_filter(uns final);
void gather_filter_undo(void);

/* ganalyse.c */

void gather_init_analyser(void);
void gather_raw_analyse(void);
int gather_analysis_needed(void);
void gather_analyse(void);

/* proto/proto.c + protocols */

void gather_download(void);
void gather_create_key(void);
void http_download(void);
void file_download(void);
u32 resolve_host_name(byte *host);

/* format/format.c */

extern const char * const parser_names[];
extern uns trace_parse, max_conversions;

struct parser_hook {
  cnode n;
  char *type_patt;
  int parser;
  char **args;				/* Dynamic array of parser arguments */
};

struct parser_hook *identify_content_type(byte *type);
struct parser_hook *identify_content_encoding(byte *enc);

/* format/parse.c */

void gather_parse(void);
void switch_content_encoding(void);
void switch_content_type(byte *what);

/* Format parsers */

#define P(x) int x##_parse(char **args);
#include "gather/format/parsers.h"
#undef P

/* format/image.c */

struct gobj_ref *image_add_ref(byte *src, byte *ww, byte *hh, uns nofollow);
void image_filter_refs(void);

/* format/validate.c */

void validate_document(void);

/* format/alloc.c */

void *parser_malloc(uns size);
void *parser_realloc(void *ptr, uns size);
void *parser_malloc_zero(uns size);

/* charset.c */

void convert_charset(byte *meta_charset);

/* content.c */

void set_content_encoding(byte *ce);
void set_content_type(byte *ct);
void parse_content_type(byte *ct, byte **charset);

void guess_content(void);
void guess_content_by_name(byte *name, byte **type, byte **enc);
void cut_inenc_suffix(byte *filename, byte *enc);

/* fb-textpacker.c */

struct fastbuf *fb_wrap_textpacker_out(struct fastbuf *dest);

/* Content-Type pattern matching and filters */

int match_ct_patt(const char *, const char *);

#endif
