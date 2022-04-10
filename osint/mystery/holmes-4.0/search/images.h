#ifndef _SEARCH_IMAGES_H
#define _SEARCH_IMAGES_H

struct database;
struct query;
struct expr;
struct options;
struct ref_context;
struct ref_buffers;
struct image_signature;
struct image_cluster;

extern uns max_image_sims, image_sim_max_weight, image_sim_slope;

#ifdef CONFIG_IMAGES_SIM

enum image_sim_type {
  IMAGE_SIM_URL,
  IMAGE_SIM_CARD_ID,
  IMAGE_SIM_SIG
};

struct image_sim_options {
  int weight;			/* parameter: maximum Q bonus */
  uns max_dist;			/* parameter: maximum similarity distance */
};

struct image_sim_match {
  enum image_sim_type type;
  struct image_sim_options o;
  union {
    struct {
      byte *url;
      struct database *db;
      uns card_id;
    };
    byte *signature;
  };
  struct image_signature *sig;
};

struct image_sim {
  struct image_sim_match m;
  int boolean_id;
  uns dist;			/* distance (filled in refs_go) */
#ifdef CONFIG_EXPLAIN
  byte *explain_pos;
#endif
};

/* used in dbase.c */
void images_init(struct database *db);

/* used in parse.y */
struct expr *image_sim_new_url(byte *url);
struct expr *image_sim_new_card_id(struct database *db, uns card_id);
struct expr *image_sim_new_sig(byte *signature);

/* used in query.c */
byte *image_sim_match_format(struct image_sim_match *s, byte *buf, byte *bend);
byte *image_sim_format(struct image_sim *s, byte *buf, byte *bend);
void image_sim_match_propagate_opts(struct image_sim_match *s, struct options *o);
struct expr *image_sim_match_analyse(struct query *q, struct expr *e);
int images_eval(struct query *q);

/* used in refs.c */
void image_bare_ref_context_init(struct ref_context *c);
void image_ref_context_init(struct ref_context *c, struct ref_context *clone);
void image_sim_explain(struct ref_context *c, struct image_sim *sim, uns card_id, void (*msg)(byte *text, void *param), void *param);

static inline uns
image_sim_q(struct image_sim *s)
{
  return (s->m.o.weight * image_sim_slope) / (image_sim_slope + (s->dist >> 1));
}

#else

struct image_sim_match {};
struct image_sim {
  int boolean_id; 
};

/* some of the following functions are never called because of undefined IMAGESIM token */

static inline void images_init(struct database *db UNUSED) {}
static inline struct expr *image_sim_new_url(byte *url UNUSED) { ASSERT(0); }
static inline struct expr *image_sim_new_card_id(struct database *db UNUSED, uns card_id UNUSED) { ASSERT(0); }
static inline  struct expr *image_sim_new_sig(byte *signature UNUSED) { ASSERT(0); }
static inline byte *image_sim_match_format(struct image_sim_match *s UNUSED, byte *buf UNUSED, byte *bend UNUSED) { ASSERT(0); }
static inline byte *image_sim_format(struct image_sim *s UNUSED, byte *buf UNUSED, byte *bend UNUSED) { ASSERT(0); }
static inline void image_sim_match_propagate_opts(struct image_sim_match *s UNUSED, struct options *o UNUSED) { ASSERT(0); }
static inline struct expr *image_sim_match_analyse(struct query *q UNUSED, struct expr *e UNUSED) { ASSERT(0); }
static inline int images_eval(struct query *q UNUSED) { return 1; }
static inline void image_bare_ref_context_init(struct ref_context *c UNUSED) {}
static inline void image_ref_context_init(struct ref_context *c UNUSED, struct ref_context *clone UNUSED) {}

#endif
#endif
