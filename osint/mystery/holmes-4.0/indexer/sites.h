/*
 *	Sherlock Indexer -- Deterministic site_id's
 *
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_SITES_H
#define _SHERLOCK_INDEXER_SITES_H

#ifdef CONFIG_SITES
#define SITE_ID_SIZE	3
#define SITE_HASH_SIZE	6
#else
#define SITE_ID_SIZE	0
#define SITE_HASH_SIZE	0
#endif

typedef char site_id_t[SITE_ID_SIZE];
typedef char site_hash_t[SITE_HASH_SIZE];

struct site_mapping
{
  // site_id is implicit, since it goes continuously from 1
  site_hash_t site_hash;
} PACKED;

#ifdef CONFIG_SITES
void site_url2name(char *url, char *name, int level);
void site_name2hash(char *name, site_hash_t hash);

struct site_mapping_table
{
  uns len;
  struct site_mapping *map;
};

int site_map(struct site_mapping_table *table, char *path);
void site_unmap(struct site_mapping_table *table);
int site_find_id(struct site_mapping_table *table, u64 site);
void site_name_compute(char *url, char *site_name, int site_level, site_hash_t site_hash);
u64 site_find_hash(struct site_mapping_table *table, uns site_id);
u64 site_name2hash_u64(char *name);

#else
static inline u64
site_name2hash_u64(char *name UNUSED)
{
  return 0;
}
#endif

#endif
