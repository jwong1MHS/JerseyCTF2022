/*
 *	Sherlock Gatherer Daemon -- Databases
 *
 *	(c) 1997--2003 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/daemon/gatherd.h"
#include "sherlock/db.h"
#include "ucw/url.h"
#include "ucw/string.h"

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/file.h>

/** THE GATHERER LOCK ***/

static int glock_fd = -1;

void
gather_lock(uns allow_override)
{
  int existed = 0;

  glock_fd = open(lock_name, O_WRONLY | O_CREAT | O_EXCL, 0666);
  if (glock_fd < 0 && errno == EEXIST)
    {
      existed = 1;
      glock_fd = open(lock_name, O_WRONLY, 0666);
    }
  if (glock_fd < 0)
    die("Cannot create gatherer lock %s: %m", lock_name);
  if (flock(glock_fd, (allow_override ? LOCK_SH : LOCK_EX) | LOCK_NB) >= 0)
    {
      if (existed)
	log(L_INFO, "Overriding stale gatherer database lock");
    }
  else
    {
      if (errno != EWOULDBLOCK)
	die("Error locking %s: %m", lock_name);
      if (!allow_override)
	die("Gatherer lock %s already locked by another process", lock_name);
      log(L_WARN, "Gatherer database already locked by another process, consistency not guaranteed");
      close(glock_fd);
      glock_fd = -1;
    }
}

void
gather_unlock(void)
{
  if (glock_fd >= 0)
    {
      flock(glock_fd, LOCK_UN);
      close(glock_fd);
      glock_fd = -1;
      if (unlink(lock_name) < 0)
	log(L_ERROR, "Cannot delete gatherer lock: %m");
    }
}

void
gather_note_state(byte *state)
{
  ASSERT(glock_fd >= 0);
  lseek(glock_fd, 0, SEEK_SET);
  ftruncate(glock_fd, 0);
  write(glock_fd, state, strlen(state));
}

/*** THE URL DATABASE ***/

static struct sdbm *urldb;

void
urldb_open(void)
{
  struct sdbm_options o;

  /* WARNING: Must correspond to parameters used by the expirer */
  o.name = urldb_name;
  o.flags = SDBM_CREAT | SDBM_WRITE;
  o.page_order = 12;	/* We need >=12 because or URL size and 12 gives the best performance */
  o.cache_size = urldb_cache_size;
  o.key_size = -1;
  o.val_size = sizeof(struct urlrec);
  if (!(urldb = sdbm_open(&o)))
    die("Cannot open the URL database");
}

void
urldb_close(void)
{
  sdbm_close(urldb);
}

int
urldb_lookup(byte *url, struct urlrec *u)
{
  uns urs = sizeof(struct urlrec);

  return sdbm_fetch(urldb, url, strlen(url), (byte *) u, &urs);
}

int
urldb_exists(byte *url)
{
  return sdbm_fetch(urldb, url, sizeof(url), NULL, NULL);
}

void
urldb_rewind(void)
{
  sdbm_rewind(urldb);
}

byte *
urldb_get_next(byte *buf, struct urlrec *u)
{
  uns keysize = MAX_URL_SIZE - 1;
  uns valsize = sizeof(struct urlrec);

  if (sdbm_get_next(urldb, buf, &keysize, (byte *) u, &valsize) > 0)
    {
      buf[keysize] = 0;
      return buf;
    }
  else
    return NULL;
}

int
urldb_delete(byte *url)
{
  return sdbm_delete(urldb, url, strlen(url));
}

void
urldb_store(byte *url, struct urlrec *ur)
{
  sdbm_replace(urldb, url, strlen(url), (byte *) ur, sizeof(struct urlrec));
}

/*** THE MD5 SUM DATABASE ***/

static struct sdbm *md5db;

void
md5db_open(void)
{
  struct sdbm_options o;

  if (!md5db_name)
    return;

  o.name = md5db_name;
  o.flags = SDBM_CREAT | SDBM_WRITE;
  o.page_order = 10;
  o.cache_size = md5db_cache_size;
  o.key_size = MD5_SIZE;
  o.val_size = sizeof(struct md5rec);
  if (!(md5db = sdbm_open(&o)))
    die("Cannot open the MD5 database");
}

void
md5db_close(void)
{
  if (md5db)
    sdbm_close(md5db);
}

int
md5db_lookup(byte *md5, struct md5rec *mr)
{
  byte m[MD5_SIZE];
  uns ms = sizeof(struct md5rec);

  if (md5db)
    {
      hex_to_mem(m, md5, MD5_SIZE, 0);
      return sdbm_fetch(md5db, m, MD5_SIZE, (byte *) mr, &ms);
    }
  return 0;
}

int
md5db_exists(byte *md5)
{
  byte m[MD5_SIZE];

  if (md5db)
    {
      hex_to_mem(m, md5, MD5_SIZE, 0);
      return sdbm_fetch(md5db, m, MD5_SIZE, NULL, NULL);
    }
  return 0;
}

void
md5db_rewind(void)
{
  if (md5db)
    sdbm_rewind(md5db);
}

byte *
md5db_get_next(byte *key, struct md5rec *mr)
{
  byte m[MD5_SIZE];
  uns keysize = MD5_SIZE;
  uns valsize = sizeof(struct md5rec);

  if (md5db && sdbm_get_next(md5db, m, &keysize, (byte *) mr, &valsize))
    {
      mem_to_hex(key, m, MD5_SIZE, MEM_TO_HEX_UPCASE);
      return key;
    }
  else
    return NULL;
}

int
md5db_delete(byte *md5)
{
  byte m[MD5_SIZE];

  if (md5db)
    {
      hex_to_mem(m, md5, MD5_SIZE, 0);
      return sdbm_delete(md5db, m, MD5_SIZE);
    }
  else
    return 0;
}

void
md5db_store(byte *md5, oid_t oid)
{
  byte m[MD5_SIZE];
  struct md5rec md = { oid };

  if (md5db)
    {
      hex_to_mem(m, md5, MD5_SIZE, 0);
      sdbm_replace(md5db, m, MD5_SIZE, (byte *) &md, sizeof(md));
    }
}

void
db_sync(void)
{
  sdbm_sync(urldb);
  sdbm_sync(md5db);
}
