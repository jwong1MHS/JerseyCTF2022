/*
 *	Sherlock Indexer -- Deterministic site_id's
 *
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/url.h"
#include "ucw/md5.h"
#include "sherlock/index.h"
#include "indexer/sites.h"

#include <string.h>

void
site_url2name(char *source_url, char *name, int level)
{
  /* Translate the URL using equivalence classes.  */
  char buf_tr[URL_KEY_BUF_SIZE];
  source_url = url_key(source_url, buf_tr);

  /* Split the URL into parts.  */
  struct url url;
  char buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  int err = url_canon_split(source_url, buf1, buf2, &url);
  if (err)
    die("url_canon_split(%s): %s", source_url, url_error(err));
  ASSERT(url.host);

  if (!level)
    strcpy(name, url.host);
  else if (level > 0)
  {
    char *c = url.host + strlen(url.host);
    while (level--)
      while (c > url.host && 0[--c] != '.');
    if (*c == '.')
      c++;
    strcpy(name, c);
  }
  else
  {
    strcpy(name, url.host);
    name += strlen(name);
    char *c = url.rest;
    while (level++)
    {
      char *d = c+1;
      while (*d && *d != '/')
	d++;
      if (!*d)
	break;
      else
	c = d;
    }
    memcpy(name, url.rest, c-url.rest+1);
    name[c-url.rest+1] = 0;
  }
}

void
site_name2hash(char *name, site_hash_t hash)
{
  md5_context md5;
  md5_init(&md5);
  md5_update(&md5, name, strlen(name));
  memcpy(hash, md5_final(&md5), SITE_HASH_SIZE);
}

int
site_map(struct site_mapping_table *table, char *path)
{
  table->map = mmap_file(path, &table->len, 0);
  if (!table->map)
    return -1;
  ASSERT(!(table->len % sizeof(struct site_mapping)));
  table->len /= sizeof(struct site_mapping);
  return 0;
}

void
site_unmap(struct site_mapping_table *table)
{
  munmap_file(table->map, table->len * sizeof(struct site_mapping));
  table->map = NULL;
}

int
site_find_id(struct site_mapping_table *table, u64 site)
{
  if (!table || !table->map)
  {
    ASSERT(site < (1LL << 32));
    return site;
  }
  site_hash_t hash;
  memcpy(&hash, &site, SITE_HASH_SIZE);
  int a=0, b=table->len;
  while (a < b)
  {
    int c = (a+b)/2;
    int cmp = memcmp(hash, &table->map[c].site_hash, SITE_HASH_SIZE);
    if (!cmp)
      return c;
    else if (cmp < 0)
      b = c;
    else
      a = c+1;
  }
  return ~0U;
}

void
site_name_compute(char *url, char *site_name, int site_level, site_hash_t site_hash)
{
  if (!site_name[0])
    site_url2name(url, site_name, site_level);
  site_name2hash(site_name, site_hash);
}

u64
site_find_hash(struct site_mapping_table *table, uns site_id)
{
  if (!table || !table->map)
    return site_id;
  ASSERT(site_id < table->len);
  u64 site = 0;
  memcpy(&site, &table->map[site_id].site_hash, SITE_HASH_SIZE);
  return site;
}

u64
site_name2hash_u64(char *name)
{
  site_hash_t hash;
  u64 L = 0;
  site_name2hash(name, hash);
  memcpy(&L, &hash, SITE_HASH_SIZE);
  return L;
}
