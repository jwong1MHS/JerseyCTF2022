/*
 *	Sherlock Search Engine -- Similar images
 *
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "sherlock/lizard-fb.h"
#include "ucw/lfs.h"
#include "ucw/base224.h"
#include "ucw/base64.h"
#include "ucw/unicode.h"
#include "ucw/unaligned.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "images/images.h"
#include "images/signature.h"
#include "images/object.h"
#include "search/sherlockd.h"
#include "search/refs.h"
#include "indexer/params.h"

#include <stdio.h>
#include <string.h>

#define IS_TRACING (current_query->debug & DEBUG_IMAGES)
#define TRACE(msg...) do { if (IS_TRACING) add_reply(msg); } while(0)

void
images_init(struct database *db)
{
  if (db->parts & DB_PART_IMAGE_SIGNATURES)
    {
      log(L_INFO, "Loading image signatures");
      byte *fn_image_signatures = db_file_name(db, "image-signatures");
      byte *fn_image_clusters = db_file_name(db, "image-clusters");
      db->fd_image_signatures = ucw_open(fn_image_signatures, O_RDONLY);
      if (db->fd_image_signatures < 0)
	die("Unable to open %s: %m", fn_image_signatures);
      struct fastbuf *fb = bopen(fn_image_clusters, O_RDONLY, 4096);
      db->image_clusters_depth = bgetl(fb);
      ASSERT(db->image_clusters_depth < 24);
      uns size = sizeof(*db->image_clusters) << db->image_clusters_depth;
      db->image_clusters = xmalloc(size);
      breadb(fb, db->image_clusters, size);
      bclose(fb);
    }
}

static struct expr *
image_sim_new(enum image_sim_type type)
{
  struct expr *e = new_node(EX_IMAGE_SIM_MATCH);
  e->u.image_sim_match.type = type;
  e->u.image_sim_match.o.weight = image_sim_max_weight;
  e->u.image_sim_match.o.max_dist = ~0U;
  return e;
}

struct expr *
image_sim_new_url(byte *url)
{
  DBG("image_sim_new_url(\"%s\")", url);
  struct expr *e = image_sim_new(IMAGE_SIM_URL);
  e->u.image_sim_match.url = url;
  return e;
}

struct expr *
image_sim_new_card_id(struct database *db, uns card_id)
{
  DBG("image_sim_new_card_id(\"%s\", 0x%x)", db->name, card_id);
  struct expr *e = image_sim_new(IMAGE_SIM_CARD_ID);
  e->u.image_sim_match.db = db;
  e->u.image_sim_match.card_id = card_id;
  return e;
}

struct expr *
image_sim_new_sig(byte *signature)
{
  DBG("image_sim_new_signature(\"%s\")", signature);
  uns len = strlen(signature);
  byte buf[len];
  uns size = base64_decode(buf, signature, len);
  if (!size || *buf > IMAGE_REG_MAX || size != image_signature_size(*buf))
    err("Invalid image signature (base64 encoded)");
  // FIXME: maybe use a human-readable format
  struct expr *e = image_sim_new(IMAGE_SIM_SIG);
  e->u.image_sim_match.signature = signature;
  e->u.image_sim_match.sig = mp_memdup(current_query->pool, buf, size);
  return e;
}

static uns
image_sim_options_format(struct image_sim_options *o, byte *buf)
{
  return sprintf(buf, " W%u D%u)", o->weight, o->max_dist);
}

byte *
image_sim_match_format(struct image_sim_match *s, byte *buf, byte *bend)
{
  switch (s->type)
    {
      case IMAGE_SIM_URL:
	if (buf + 64 + strlen(s->url) > bend)
	  return NULL;
	buf += sprintf(buf, "IMS(\"%s\"", s->url);
	buf += image_sim_options_format(&s->o, buf);
        break;
      case IMAGE_SIM_CARD_ID:
	if (buf + 64 + strlen(s->db->name) > bend)
	  return NULL;
	buf += sprintf(buf, "IMS(DB \"%s\" %x", s->db->name, s->card_id);
	buf += image_sim_options_format(&s->o, buf);
        break;
      case IMAGE_SIM_SIG:
	if (buf + 64 + strlen(s->signature) > bend)
	  return NULL;
	buf += sprintf(buf, "IMS(SIG \"%s\"", s->signature);
	buf += image_sim_options_format(&s->o, buf);
        break;
      default:
	ASSERT(0);
    }
  return buf;
}

byte *
image_sim_format(struct image_sim *s, byte *buf, byte *bend)
{
  return image_sim_match_format(&s->m, buf, bend);
}

void
image_sim_match_propagate_opts(struct image_sim_match *s UNUSED, struct options *o UNUSED)
{
  /* IMAGESIM token currently understands no extra options */
}

struct expr *
image_sim_match_analyse(struct query *q, struct expr *e)
{
  if (!q->dbase->image_clusters_depth)
    return new_node(EX_NONE);
  ASSERT(q->nimage_sims < max_image_sims);
  struct expr *n = new_node(EX_IMAGE_SIM);
  struct image_sim *s = mp_alloc_zero(q->results->pool, sizeof(*s));
  n->u.image_sim = q->image_sims[q->nimage_sims++] = s;
  s->m = e->u.image_sim_match;
  return n;
}

static int
images_eval_expr(struct query *q, struct expr *e)
{
  switch (e->type)
    {
      case EX_AND:
      case EX_OR:
	return images_eval_expr(q, e->u.op.l) && images_eval_expr(q, e->u.op.r);
      case EX_NOT:
	return images_eval_expr(q, e->u.op.l);

      case EX_IMAGE_SIM_MATCH:
	{
	  struct image_sim_match *s = &e->u.image_sim_match;
          if (q->nimage_sims++ >= max_image_sims)
            {
	      /* Avoid too many seeks */
              add_err("-124 Too many IMAGESIMs");
	      return 0;
            }
	  switch (e->u.image_sim_match.type)
	    {
	      case IMAGE_SIM_URL:
	        DBG("Resolving URL %s", s->url);
	        CLIST_FOR_EACH_BACKWARDS(struct database *, db, databases)
	          if (db->parts & DB_PART_STRINGS)
	            {
		      DBG("Searching in DB %s", db->name);
		      ucw_off_t refchain_start;
		      uns refchain_len;
		      if (string_db_find_refchain(q, db, s->url, &refchain_start, &refchain_len))
		        {
		          byte *refchain_pos = mmap_region(q, db->fd_refs, refchain_start, MIN(refchain_start + refchain_len, db->ref_file_size));
		          if (!refchain_pos)
			    {
			      DBG("Cannot map refchain");
			      add_err("-125 Cannot resolve signature (refchain map error)");
			      return 0;
			    }
			  uns card_id, found_mask;
			  if (card_id = refs_chain_find_type_mask(&refchain_pos, 1 << ST_URL, 0, &found_mask, db->params->num_slices > 1))
			    {
			      s->card_id = card_id;
			      s->db = db;
			      goto url_resolved;
			    }
		        }
		    }
		  DBG("URL not found");
		  add_err("-125 Cannot resolve signature (URL not found)");
		  return 0;
url_resolved:
		  /* fall-thru */

	      case IMAGE_SIM_CARD_ID:
	        DBG("Resolving DB:CARDID %s:#%x", s->db->name, s->card_id);
                if (!s->card_id || s->card_id >= s->db->num_ids)
                  {
		    DBG("CARDID out of range");
	            add_err("-125 Cannot resolve signature (CARDID out of database range)");
	            return 0;
	          }
                struct card_attr *ca = &s->db->card_attrs[s->card_id];
                ucw_off_t card_start = (ucw_off_t) ca->card << CARD_POS_SHIFT;
                ucw_off_t card_end = (ucw_off_t) (ca + 1)->card << CARD_POS_SHIFT;
		DBG("Mapping cards zone 0x%llx-0x%llx", (long long)card_start, (long long)card_end);
	        byte *card_map = mmap_region(q, s->db->fd_cards, card_start, card_end);
	        if (!card_map)
	          {
		    DBG("Cannot map card");
	            add_err("-125 Cannot resolve signature (card map error)");
	            return 0;
		  }
	        byte *ptr;
	        uns type;
	        int len = lizard_memread(liz_buf, card_map, &ptr, &type);
	        if (len < 0)
		  die("Cannot decompress object %08x in database %s", s->card_id, s->db->name);
	        get_attr_set_type(type);
	        struct parsed_attr pa;
	        uns depth = 0;
	        for (;;)
      	          {
		    if (get_attr(&ptr, ptr + len, &pa) <= 0)
      		      {
		        DBG("No 'H' attribute");
		        add_err("-125 Cannot resolve signature (not image)");
		        return 0;
		      }
		    if (pa.attr == '(')
		      depth++;
		    else if (pa.attr == ')')
		      depth--;
		    else if (pa.attr == 'H' && !depth)
		      {
	                byte buf[pa.len];
	                uns size = base224_decode(buf, pa.val, pa.len);
	                ASSERT(*buf <= IMAGE_REG_MAX && size == image_signature_size(*buf));
	                s->sig = mp_memdup(q->pool, buf, size);
	                DBG("Signature resolved (len=%u)", s->sig->len);
		        break;
		      }
		  }
	        /* fall-thru */

	      case IMAGE_SIM_SIG:
	        /* Already done */
	        break;

	      default:
	        ASSERT(0);
	    }
	  return 1;
	}
      default:
	return 1;
    }
}

int
images_eval(struct query *q)
{
  DBG("images_eval()");
  q->nimage_sims = 0;
  return images_eval_expr(q, q->expr);
}

void
image_bare_ref_context_init(struct ref_context *c)
{
  DBG("image_bare_ref_context_init()");
  struct query *q = c->query;
  c->nimage_sims = q->nimage_sims;
  c->image_sims = mp_alloc(q->pool, q->nimage_sims * sizeof(struct image_sim));
  for (uns i = 0; i<q->nimage_sims; i++)
    c->image_sims[i] = *q->image_sims[i];
}

void
image_ref_context_init(struct ref_context *c, struct ref_context *clone)
{
  if (clone)
    {
      c->image_sims_bool_mask = clone->image_sims_bool_mask;
      return;
    }

  DBG("image_ref_context_init()");
  struct query *q = c->query;
  uns nsims = c->nimage_sims;
  if (!nsims)
    return;

  struct image_sim *sims = c->image_sims;
  struct database *db = q->dbase;
  struct image_cluster *clusters = db->image_clusters;
  struct mmap_request mmap_array[nsims];
  struct mmap_request *mmaps[nsims];

  /* Map clusters to search in */
  for (uns i = 0; i < nsims; i++)
    {
      struct image_sim *sim = &sims[i];
      struct mmap_request *m = &mmap_array[i];
      struct image_signature *sig = sim->m.sig;

      /* Find the cluster in search tree */
      struct image_cluster *clus = clusters;
      uns index = 0;
      for (uns i = 1; i < db->image_clusters_depth; i++)
        {
	  int dot = 0;
	  for (uns j = 0; j < IMAGE_VEC_F; j++)
	    dot += (int)sig->vec.f[j] * clus->vec[j];
	  index = index * 2 + ((dot <= clus->dot) ? 1 : 2);
	  clus = clusters + index;
	}

      /* Map signatures */
      m->u.req.fd = db->fd_image_signatures;
      m->u.req.start = (ucw_off_t) clus[0].pos;
      m->u.req.end = (ucw_off_t) clus[1].pos;
      m->userdata = i;
      DBG("Mapping image-signatures zone %llx-%llx", (long long)clus[0].pos, (long long)clus[1].pos);
    }
  if (mmap_regions(q, mmap_array, nsims) < 0)
    {
      DBG("Cannot map image signatures");
      add_err("-117 Too many documents match");
      eval_err(117);
    }
  for (uns i = 0; i < nsims; i++)
    mmaps[mmap_array[i].userdata] = &mmap_array[i];

  /* Create synthetic chains:
   *
   *    byte	slice_mask
   *    utf8	slice_sizes[]
   *	sequence of:	u28	card_id
   * 			u4	chain_len (=4)
   * 			u32	image_distance
   *	u32	zero
   */

  bb_t *bb = &ref_buffers->image_sims_chains;
  bb_grow(bb, 4096);
  uns bb_len = 0;

  /* FIXME:
   * - do only once, not for each slice */
  uns *slice_start = db->slice_start;

  uns start[nsims];
  for (uns i = 0; i < nsims; i++)
    {
      struct image_sim *sim = &sims[i];
      struct image_signature *sig1 = sim->m.sig;
      byte *p = mmaps[i]->u.map.start;
      byte *end = mmaps[i]->u.map.end;
#ifdef CONFIG_EXPLAIN
      sim->explain_pos = p;
#endif

      /* Allocate enough space for chain header (slice_mask, UTF-8 encoded lengths).
       * We keep entries aligned. */
      bb_len += ALIGN_TO(1 + 6 * HARD_MAX_SLICES, 4);
      bb_grow(bb, bb_len);
      uns slice_id = 0, slice_pos = bb_len;
      byte head[1 + 6 * HARD_MAX_SLICES], *head_pos = head + 1;
      *head = 0;
      start[i] = bb_len;

      /* Always match the same image (we may miss it
       * in rare cases of the image laying exactly in a cutting plane
       * or if it is too small for segmentation). */
      uns query_card_id = (sim->m.type != IMAGE_SIM_SIG && db == sim->m.db) ? sim->m.card_id : ~0U;
      DBG("Creating synthetic IMAGESIM chain (query_card_id=#%x)", query_card_id);
      while (p != end || query_card_id != ~0U)
        {
	  uns dist = 0, card_id;
	  if (p == end)
	    {
	      card_id = query_card_id;
	      query_card_id = ~0U;
	    }
	  else
	    {
	      card_id = GET_U32(p);
	      if (query_card_id <= card_id)
	        {
	          if (query_card_id < card_id)
		    card_id = query_card_id;
		  else
		    p += 4 + image_signature_size(p[4]);
		  query_card_id = ~0U;
		}
	      else
	        {
		  p += 4;
		  dist = image_signatures_dist(sig1, (struct image_signature *)p);
		  p += image_signature_size(*p);
		}
	    }

	  /* Insert (card_id, dist) */
	  u32 *pos = (u32 *)(bb_grow(bb, bb_len + 12) + bb_len);
	  while (card_id >= slice_start[slice_id + 1])
	    {
	      if (bb_len != slice_pos)
	        {
		  *head |= 1 << slice_id++;
		  *pos++ = 0;
                  bb_len += 4;
		  head_pos = utf8_32_put(head_pos, bb_len - slice_pos);
		  slice_pos = bb_len;
		}
	    }
	  pos[0] = card_id | 0x40000000;
	  pos[1] = dist;
  	  bb_len += 8;
	}
      *head |= 1 << slice_id;
      byte *pos = bb_grow(bb, bb_len + 4);
      *(u32 *)(pos + bb_len) = 0;
      bb_len += 4;
      uns head_len = head_pos - head;
      if (db->params->num_slices > 1)
	{
	  start[i] -= head_len;
	  memcpy(pos + start[i], head, head_len);
        }
    }

  /* XXX: We store synthetic chains in struct chain to allow merging in refs_go() with
   *      normal refchains. See refs_go() for details. */
  uns mask = 0;
  for (uns i = 0; i < nsims; i++)
    {
      struct chain *ch = &c->raw_chains[c->num_raw_chains++];
      struct image_sim *sim = &sims[i];
      ch->pos = bb->ptr + start[i];
      ch->bool_index = sim->boolean_id;
      ch->word_index = i;
      mask |= (1 << sim->boolean_id);
    }
  c->image_sims_bool_mask = mask;
}

void
image_sim_explain(struct ref_context *c UNUSED, struct image_sim *sim UNUSED, uns card_id UNUSED, void (*msg)(byte *text, void *param) UNUSED, void *param UNUSED)
{
#ifndef CONFIG_EXPLAIN
  ASSERT(0);
#else
  DBG("image_sim_explain(sim=%p card_id=0x%x)", sim, card_id);
  struct image_signature *sig;
  if (sim->m.type != IMAGE_SIM_SIG && c->query->dbase == sim->m.db && card_id == sim->m.card_id)
    sig = sim->m.sig;
  else
    {
      byte *p = sim->explain_pos;
      for (;;)
        {
          uns id = GET_U32(p);
          p += 4;
          uns len = *p;
          uns size = image_signature_size(len);
          sig = (struct image_signature *)p;
          p += size;
          if (id == card_id)
	    break;
        }
      sim->explain_pos = p;
    }
  image_signatures_dist_explain(sim->m.sig, sig, msg, param);
#endif
}
