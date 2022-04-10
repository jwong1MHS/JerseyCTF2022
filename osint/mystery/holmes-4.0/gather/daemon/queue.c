/*
 *	Sherlock Gatherer Daemon -- Queue Management
 *
 *	(c) 1997--2003 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "gather/daemon/gatherd.h"
#include "ucw/url.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "sherlock/pagecache.h"
#include "ucw/lfs.h"
#include "ucw/unaligned.h"
#include "ucw/hashfunc.h"
#include "ucw/heap.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/*** HOST QUEUE ***/

/*
 *  The data structures are somewhat complicated due to our requirements
 *  for per-host queueing as opposed to per-IP-address timing.
 *
 *  We keep a set of qnodes (they correspond to IP addresses, some of them
 *  are reserved for hosts with not yet resolved address), each qnode contains
 *  a linked list of hosts. No two active hosts can share a qnode. Timings
 *  are defined on qnodes, item queues are connected to hosts.
 *
 *  qnode states:
 *     -  QS_IDLE (has no hosts; linked in idle_nodes list)
 *     -  QS_ACTIVE (linked in active_nodes list)
 *     -  QS_WAITING (stored in waiting_node_heap)
 *     -  QS_READY (stored in ready_node_heap)
 *
 *  host states:
 *     -  HS_IDLE (no items queued; linked in idle_hosts list)
 *     -  HS_ACTIVE (linked in active_hosts list), the qkey can be altered now
 *     -  HS_WAITING (linked in its waiting or ready qnode's heap)
 *
 *  One of the hosts (pointed to by free_list) serves as dummy header
 *  of the list of free queue file blocks. It's always kept in the
 *  idle_hosts list.
 *
 *  Assignment of queue keys (see also gather_create_key())
 *     00000000 - 00ffffff	Hosts waiting for resolving of real key
 *     7f010000 - 7f01ffff	Non-IP hosts (file:// etc.)
 *     7f020000 - 7f02ffff	Keys for unresolvable hosts
 *     everything else		IP address of the host
 *
 *  Resolver nodes (keys 00xxxxxx) take precedence over normal nodes (this is
 *  achived by their wait time being always zero), but normal nodes cannot starve
 *  as the number of resolver nodes is limited by max_resolvers.
 */

static int flat_queue_mode;		/* Don't use qkeys and consider all hosts active */

/* The heap of waiting queue nodes: ordered by wakeup time */

#define BH_PREFIX(x) waiting_heap_##x
#define BH_WANT_INSERT
#define BH_WANT_FINDMIN
#define BH_WANT_DELETEMIN
struct bh_heap waiting_node_heap;
static inline int waiting_heap_less(struct bh_node *a, struct bh_node *b)
{
  struct qnode *x = (struct qnode *) a;
  struct qnode *y = (struct qnode *) b;
  return (x->wait_until < y->wait_until);
}
#include "ucw/binheap.h"

/* The heap of ready queue nodes: ordered by priority and then by sequence numbers */

static struct qnode **ready_heap;
static uns ready_heap_n, ready_heap_max;

#define RHEAP_LESS(x,y) ((x)->qpriority > (y)->qpriority || ((x)->qpriority == (y)->qpriority && (x)->sequence > (y)->sequence))
#define RHEAP_SWAP(h,i,j,t) do { t=h[i]; h[i]=h[j]; h[j]=t; h[i]->link.heap_index=i; h[j]->link.heap_index=j; } while (0)

static void
ready_heap_init(void)
{
  ready_heap_n = 0;
  ready_heap_max = 1;
  ready_heap = xmalloc(sizeof(struct qnode *) * (ready_heap_max+1));
}

static void
ready_heap_insert(struct qnode *n)
{
  if (ready_heap_n >= ready_heap_max)
    {
      ready_heap_max = 2*ready_heap_max;
      DBG("Re-allocating ready heap to %d entries", ready_heap_max);
      ready_heap = xrealloc(ready_heap, sizeof(struct qnode *) * (ready_heap_max+1));
    }
  ready_heap[++ready_heap_n] = n;
  n->link.heap_index = ready_heap_n;
  HEAP_INSERT(struct qnode *, ready_heap, ready_heap_n, RHEAP_LESS, RHEAP_SWAP);
}

static struct qnode *
ready_heap_deletemin(void)
{
  if (!ready_heap_n)
    return NULL;
  HEAP_DELMIN(struct qnode *, ready_heap, ready_heap_n, RHEAP_LESS, RHEAP_SWAP);
  return ready_heap[ready_heap_n+1];
}

static void
ready_heap_delete(struct qnode *n)
{
  HEAP_DELETE(struct qnode *, ready_heap, ready_heap_n, RHEAP_LESS, RHEAP_SWAP, n->link.heap_index);
}

/* The heap of hosts in each queue node: ordered by priority and then by sequence numbers */

#define BH_PREFIX(x) host_heap_##x
#define BH_WANT_INSERT
#define BH_WANT_FINDMIN
#define BH_WANT_DELETEMIN
static inline int host_heap_less(struct bh_node *a, struct bh_node *b)
{
  struct qhost *x = (struct qhost *) a;
  struct qhost *y = (struct qhost *) b;
  if (x->qpriority > y->qpriority)
    return 1;
  if (x->qpriority < y->qpriority)
    return 0;
  return (x->sequence < x->sequence);
}
#include "ucw/binheap.h"

static clist idle_nodes;		/* Queue nodes in state QS_IDLE */
static clist active_nodes;		/* Queue nodes in state QS_ACTIVE */
static clist idle_hosts;		/* Hosts in state HS_IDLE */
static clist active_hosts;		/* Hosts in state HS_ACTIVE */

static struct qnode **node_hash;
static struct qhost **host_hash;

static struct qhost *free_list;
static struct fastbuf *host_file;
static struct mempool *host_pool;
static uns sequence_counter;
static uns host_file_size;
uns host_count;

static inline uns			/* Calculate hash value of given string */
name_hash(byte *name)
{
  return hash_string(name) & (host_hash_size - 1);
}

static inline uns
key_hash(u32 a)
{
  a ^= a >> 16;
  a ^= a << 10;
  return a & (key_hash_size - 1);
}

static void				/* Write host entry at current position of host file */
write_host_entry(struct qhost *h)
{
  struct fastbuf *f = host_file;

  bputc(f, h->protocol);
  bputw(f, h->port);
  bputs(f, h->name);
  bputc(f, '\n');
  h->hf_pos = btell(f);
  bputl(f, h->qf_pos);
  bputl(f, h->qf_last);
  bwrite(f, h->obj_count, sizeof(h->obj_count));
  bputl(f, h->robot_id);
  bputl(f, h->robot_time);
  bputl(f, h->rec_err_count);
  bputl(f, h->qkey);
  bputl(f, h->qpriority);
}

static void				/* Rewrite host entry with new data */
flush_host_entry(struct qhost *h)
{
  if (!(h->flags & QHF_DIRTY))
    return;
  DBG("Flushing <%d:%s:%d> qfp=%x qfl=%x", h->protocol, h->name, h->port, h->qf_pos, h->qf_last);
  h->flags &= ~QHF_DIRTY;
  if (h->flags & QHF_ALLOCATE)
    {
      h->flags &= ~QHF_ALLOCATE;
      bsetpos(host_file, host_file_size);
      write_host_entry(h);
      host_file_size = btell(host_file);
      return;
    }
  bflush(host_file);
  bseek(host_file, h->hf_pos, SEEK_SET);
  bputl(host_file, h->qf_pos);
  bputl(host_file, h->qf_last);
  bwrite(host_file, h->obj_count, sizeof(h->obj_count));
  bputl(host_file, h->robot_id);
  bputl(host_file, h->robot_time);
  bputl(host_file, h->rec_err_count);
  bputl(host_file, h->qkey);
  bputl(host_file, h->qpriority);
  bflush(host_file);
}

void					/* Schedule host entry for re-writing */
touch_host(struct qhost *h)
{
  DBG("Touching <%d:%s:%d>", h->protocol, h->name, h->port);
  h->flags |= QHF_DIRTY;
}

static struct qnode *			/* Lookup qnode with a given qkey or create it */
lookup_qnode(u32 qkey)
{
  struct qnode *n;
  uns hash = key_hash(qkey);
  for (n=node_hash[hash]; n && n->qkey != qkey; n=n->hash_next)
    ;
  if (!n)
    {
      n = mp_alloc_zero(host_pool, sizeof(struct qnode));
      n->hash_next = node_hash[hash];
      node_hash[hash] = n;
      n->qkey = qkey;
      n->state = QS_IDLE;
      clist_add_tail(&idle_nodes, &n->link.list_node);
      host_heap_init(&n->host_heap);
    }
  return n;
}

static void
relink_qnode(struct qhost *h, ucw_time_t wait_until, int insert_host)
{
  /*
   *  Relink the qnode corresponding to a given host to the appropriate list / heap.
   *  If insert_host is set, insert the host to the node's waiting heap.
   */

  /* Find the qnode */
  u32 qkey = h->qkey;
  if (!qkey)
    qkey = (name_hash(h->name) % max_resolvers) + 1;
  struct qnode *n = lookup_qnode(qkey);
  DBG("\tQkey %08x in state %d, our host pri %d seq %d", qkey, n->state, h->qpriority, h->sequence);

  /* Insert our host to the node's heap */
  if (insert_host)
    host_heap_insert(&n->host_heap, &h->link.heap_node);

  /* Unlink the node or decide it will remain as it is */
  switch (n->state)
    {
    case QS_ACTIVE:
      if (n->active_host != h)
	{
	  DBG("\tQkey already active with a different node");
	  return;
	}
      /* fall-thru */
    case QS_IDLE:
      clist_remove(&n->link.list_node);
      break;
    case QS_WAITING:
      DBG("\tQkey already waiting");
      return;
    case QS_READY:
      ready_heap_delete(n);
      break;
    default:
      ASSERT(0);
    }

  /* Determine node priority */
  struct qhost *best = (struct qhost *) host_heap_findmin(&n->host_heap);
  if (!best)
    {
      ASSERT(!insert_host);
      n->state = QS_IDLE;
      clist_add_tail(&idle_nodes, &n->link.list_node);
      DBG("\tQS_IDLE");
      return;
    }
  else if (qkey < NUM_RESOLVER_KEYS)	/* Always prefer resolvers */
    n->qpriority = ~0U;
  else
    n->qpriority = best->qpriority;
  n->sequence = sequence_counter++;

  /* Enqueue us according to the timeout to either ready or waiting qnodes */
  if (wait_until > now)
    {
      n->wait_until = wait_until;
      n->state = QS_WAITING;
      waiting_heap_insert(&waiting_node_heap, &n->link.heap_node);
      DBG("\tQS_WAITING for %d sec with pri %d", (int)(wait_until-now), n->qpriority);
    }
  else
    {
      n->state = QS_READY;
      ready_heap_insert(n);
      DBG("\tQS_READY with pri %d", n->qpriority);
    }
}

void
finish_host(struct qhost *h, uns delay, u32 new_qkey)
{
  DBG("Finishing host <%d:%s:%d>, delay=%d, state=%d, new_qkey=%08x", h->protocol, h->name, h->port, delay, h->state, new_qkey);
  ASSERT(h->state == HS_ACTIVE);

  /* First of all, unlink the host node from its original place */
  clist_remove(&h->link.list_node);

  /* If the qkey has changed, we need to relink the original qnode */
  if (new_qkey && new_qkey != h->qkey)
    {
      relink_qnode(h, now, 0);
      h->qkey = new_qkey;
      touch_host(h);
    }

  /* Check if we have anything to do */
  if (!h->qf_pos)
    {					/* Idle host */
      h->state = HS_IDLE;
      relink_qnode(h, now, 0);
      clist_add_tail(&idle_hosts, &h->link.list_node);
    }
  else
    {
      h->state = HS_WAITING;
      relink_qnode(h, now+delay, 1);
    }

  if (auto_sync == 1)
    flush_host_entry(h);
}

void
put_host(struct qhost *h)
{
  DBG("Putting host <%d:%s:%d>, state=%d", h->protocol, h->name, h->port, h->state);
  if (auto_sync == 1)
    flush_host_entry(h);
  if (h->state == HS_IDLE && h->qf_pos)	/* The host need activation */
    {
      clist_remove(&h->link.list_node);
      h->state = HS_WAITING;
      relink_qnode(h, 0, 1);
    }
}

int
host_time_step(ucw_time_t *p_wait_seconds)
{
  struct qnode *q;

  DBG("host_time_step (now=%d)", (int)now);
  while (q = (struct qnode *) waiting_heap_findmin(&waiting_node_heap))
    {
      if (q->wait_until > now)
	{
	  DBG("\tWaiting until %d", (int)q->wait_until);
	  *p_wait_seconds = q->wait_until - now;
	  return 1;
	}
      waiting_heap_deletemin(&waiting_node_heap);
      DBG("\tWoken up qkey %08x", q->qkey);
      ASSERT(q->state == QS_WAITING);
      q->state = QS_READY;
      ready_heap_insert(q);
    }
  DBG("\tNo more nodes.");
  return 0;
}

struct qhost *
dequeue_host(struct qnode **pnode)
{
  struct qhost *h;
  struct qnode *n;

  n = (struct qnode *) ready_heap_deletemin();
  if (!n)
    {
      DBG("Nothing to dequeue");
      return NULL;
    }
  DBG("Dequeued qkey %08x pri %d", n->qkey, n->qpriority);
  ASSERT(n->state == QS_READY);
  n->state = QS_ACTIVE;
  clist_add_tail(&active_nodes, &n->link.list_node);

  h = (struct qhost *) host_heap_deletemin(&n->host_heap);
  ASSERT(h && h->state == HS_WAITING);
  h->state = HS_ACTIVE;
  n->active_host = h;
  clist_add_tail(&active_hosts, &h->link.list_node);
  DBG("Dequeued host <%d:%s:%d> pri %d", h->protocol, h->name, h->port, h->qpriority);

  *pnode = n;
  return h;
}

struct qhost *				/* Find (possibly queued) host by its signature */
find_host(uns proto, byte *name, uns port)
{
  struct qhost *h;

  for(h = host_hash[name_hash(name)];
      h && (h->protocol != proto || h->port != port || strcmp(h->name, name));
      h = h->hash_next)
    ;
  return h;
}

struct qhost *				/* Create new host entry */
new_host(uns proto, byte *name, uns port)
{
  struct qhost *h = mp_alloc_zero(host_pool, sizeof(struct qhost) + strlen(name));
  uns hh = name_hash(name);

  h->hash_next = host_hash[hh];
  host_hash[hh] = h;
  h->port = port;
  h->protocol = proto;
  h->flags = QHF_DIRTY | QHF_ALLOCATE;
  h->robot_id = OID_UNDEFINED;
  strcpy(h->name, name);
  if (flat_queue_mode)
    {
      h->state = HS_ACTIVE;
      clist_add_tail(&active_hosts, &h->link.list_node);
    }
  else
    {
      h->state = HS_IDLE;
      clist_add_tail(&idle_hosts, &h->link.list_node);
    }
  host_count++;
  return h;
}

static void
walk_hosts_node(struct qnode *n, void (*f)(struct qhost *h))
{
  BH_FOR_ALL(host_heap_, &n->host_heap, x)
    {
      f((struct qhost *) x);
    }
  BH_END_FOR;
}

void
walk_hosts(void (*f)(struct qhost *h))
{
  struct qhost *h;
  struct qnode *n;

  for (uns i=1; i<=ready_heap_n; i++)
    walk_hosts_node(ready_heap[i], f);
  BH_FOR_ALL(waiting_heap_, &waiting_node_heap, x)
    {
      walk_hosts_node((struct qnode *) x, f);
    }
  BH_END_FOR;
  CLIST_WALK(n, active_nodes)
    walk_hosts_node(n, f);
  CLIST_WALK(h, idle_hosts)
    f(h);
  CLIST_WALK(h, active_hosts)
    f(h);
}

static void
flush_host_cache(void)
{
  if (auto_sync != 1)			/* otherwise they are already flushed */
    walk_hosts(flush_host_entry);
}

static void
reset_hosts(void)
{
  struct qhost *h;

  CLIST_WALK(h, active_hosts)
    {
      DBG("Resetting %d:%s:%d", h->protocol, h->name, h->port);
      h->qf_pos = h->qf_last = 0;
      h->qpriority = 0;
      bzero(h->obj_count, sizeof(h->obj_count));
    }
}

/* ITEM QUEUE */

static int queue_fd;			/* File handle for the queue file */
static struct page_cache *queue_cache;
static uns queue_size;

static uns				/* Return pointer to next page */
qbuf_get_next(struct page *p)
{
  return GET_U32(p->data);
}

static void
qbuf_set_next(struct page *p, uns next)	/* Set pointer to next page */
{
  PUT_U32(p->data, next);
}

static struct page *			/* Allocate new page in the queue and lock it */
qblock_alloc(void)
{
  struct page *b;
  uns pos;

  if (pos = free_list->qf_pos)
    {
      pos &= ~QUEUE_PAGE_MASK;
      b = pgc_read(queue_cache, queue_fd, pos);
      free_list->qf_pos = qbuf_get_next(b);
      touch_host(free_list);
      DBG("\tAllocated block %x from free list", pos);
    }
  else
    {
      pos = queue_size;
      queue_size += QUEUE_PAGE_SIZE;
      if (queue_size >= 0xffff0000)
	die("Oops, queue file too large, consult a wizard.");
      b = pgc_get(queue_cache, queue_fd, pos);
      DBG("\tAllocated block %x from end of file", pos);
    }
  bzero(b->data, QUEUE_PAGE_SIZE);
  return b;
}

static void				/* Return page to the page pool */
qblock_free(struct page *b)
{
  DBG("\tReturning block %x to the pool", (int) b->pos);
  bzero(b->data, QUEUE_PAGE_SIZE);
  qbuf_set_next(b, free_list->qf_pos);
  free_list->qf_pos = b->pos + 4;	/* Beware of page 0 */
  touch_host(free_list);
  pgc_mark_dirty(queue_cache, b);
}

static struct qitem *			/* Get one item without dequeueing it first */
do_peek_item(struct qhost *h, struct qitem *qi)
{
  uns pos;
  struct page *b;
  byte *d;

  DBG("peek_item(%d:%s:%d)", h->protocol, h->name, h->port);
  ASSERT(h->state == QS_ACTIVE);
  while (pos = h->qf_pos)
    {
      DBG("\t...peeking at %x", pos);
      b = pgc_read(queue_cache, queue_fd, pos & ~QUEUE_PAGE_MASK);
      pos &= QUEUE_PAGE_MASK;
      if (b->data[pos])
	{
	  d = qi->text;
	  while (*d = b->data[pos])
	    d++, pos++;
	  pos++;
	  qi->priority = GET_U32(&b->data[pos]);
	  pos += 4;
	  qi->aux = b->pos + pos;
	  pgc_put(queue_cache, b);
	  return qi;
	}
      pos = qbuf_get_next(b);
      h->qf_pos = pos;
      qblock_free(b);
      pgc_put(queue_cache, b);
      if (!pos)
	h->qf_last = 0;
      touch_host(h);
    }
  return NULL;
}

struct qitem *
peek_item(struct qhost *h)
{
  static struct qitem *qi = NULL;
  if (!qi)
    qi = xmalloc(sizeof(struct qitem));
  return do_peek_item(h, qi);
}

struct qitem *				/* Fetch item and dequeue it */
dequeue_item(struct qhost *h)
{
  struct qitem *qi = peek_item(h);
  if (qi)
    {
      struct qitem next, *n;
      DBG("\tDequeued");
      h->qf_pos = qi->aux;
      if (n = do_peek_item(h, &next))
	{
	  DBG("\tUpdating queue priority to %d", n->priority);
	  h->qpriority = n->priority;
	}
      else
	{
	  DBG("\tSeeing no future items");
	  h->qf_pos = h->qf_last = 0;
	  h->qpriority = 0;
	}
      touch_host(h);
    }
  return qi;
}

void					/* Add new item to the queue */
enqueue_item(struct qhost *h, byte *name, uns priority)
{
  struct page *b;
  uns last;
  uns slen = strlen(name) + 1;
  uns len = slen + 4;

  DBG("enqueue_item(%d:%s:%d,%s,pri=%u)", h->protocol, h->name, h->port, name, priority);
  if (!h->qf_last)
    {
      b = qblock_alloc();
      last = 4;
      DBG("\t...allocated first block at %x", (int) b->pos);
    }
  else
    {
      b = pgc_read(queue_cache, queue_fd, h->qf_last & ~QUEUE_PAGE_MASK);
      last = h->qf_last & QUEUE_PAGE_MASK;
      DBG("\t...block at %x, last=%x", (int) b->pos, last);
      if (last + len >= QUEUE_PAGE_SIZE)
	{
	  struct page *o = b;
	  b = qblock_alloc();
	  qbuf_set_next(o, b->pos + 4);
	  pgc_mark_dirty(queue_cache, o);
	  pgc_put(queue_cache, o);
	  last = 4;
	  DBG("\t...overflow, new block at %x, last=%x", (int) b->pos, last);
	}
    }
  memcpy(b->data+last, name, slen);
  PUT_U32(b->data+last+slen, priority);
  b->data[last+len] = 0;
  if (!h->qf_pos)
    h->qf_pos = b->pos + last;
  h->qf_last = b->pos + last + len;
  pgc_mark_dirty(queue_cache, b);
  pgc_put(queue_cache, b);
  if (!h->qpriority && h->state != HS_WAITING)
    h->qpriority = priority;
  touch_host(h);
}

void
requeue_item(struct qhost *h)
{
  struct qitem *q;

  q = dequeue_item(h);
  if (q)
    enqueue_item(h, q->text, q->priority);
}

uns
queue_walk_start(struct qhost *h)
{
  return h->qf_pos;
}

uns
queue_walk_next(uns *pos, struct qitem *qi)
{
  static struct page *b;
  uns p = *pos;
  byte *t, *start;

  for(;;)
    {
      if (b && (!p || b->pos != (ucw_off_t) (p & ~QUEUE_PAGE_MASK)))
	{
	  pgc_put(queue_cache, b);
	  b = NULL;
	}
      if (!p)
	{
	  *pos = 0;
	  return 0;
	}
      if (!b)
	b = pgc_read(queue_cache, queue_fd, p & ~QUEUE_PAGE_MASK);
      t = start = b->data + (p & QUEUE_PAGE_MASK);
      if (*t)
	{
	  while (*t++)
	    ;
	  memcpy(qi->text, start, t-start);
	  qi->priority = GET_U32(t);
	  t += 4;
	  *pos = p + (t - start);
	  return 1;
	}
      p = GET_U32(b->data);
    }
}

void
queue_reset(void)
{
  DBG("Resetting the queue");
  ASSERT(flat_queue_mode);
  reset_hosts();
  pgc_cleanup(queue_cache);
  ucw_ftruncate(queue_fd, 0);
  queue_size = 0;
}

/* INITIALIZATION AND CLEANUP */

static void				/* Load list of all hosts from the host file */
load_host_list(void)
{
  struct fastbuf *f;

  DBG("Loading host list");
  host_file = f = bopen(host_file_name, O_RDWR | O_CREAT, 4096);
  if (bgetc(f) < 0)
    {
      log(L_INFO, "Host database doesn't exist, creating new one");
      free_list = new_host(0, "", 0);
      flush_host_entry(free_list);
    }
  else
    {
      bsetpos(f, 0);
      for(;;)
	{
	  int proto, port;
	  struct qhost *h;
	  byte name[MAX_URL_SIZE];

	  proto = bgetc(f);
	  if (proto < 0)
	    break;
	  port = bgetw(f);
	  if (!bgets(f, name, MAX_URL_SIZE))
	    die("Malformed host file: truncated record");
	  h = new_host(proto, name, port);
	  h->hf_pos = btell(f);
	  h->qf_pos = bgetl(f);
	  h->qf_last = bgetl(f);
	  breadb(f, h->obj_count, sizeof(h->obj_count));
	  h->robot_id = bgetl(f);
	  h->robot_time = bgetl(f);
	  h->rec_err_count = bgetl(f);
	  h->qkey = bgetl(f);
	  h->qpriority = bgetl(f);
	  h->flags = 0;
	  if (!h->port && !h->protocol)
	    {
	      if (free_list)
		die("Malformed host file: multiple free list entries");
	      free_list = h;
	    }
	  else
	    put_host(h);
	}
      host_file_size = btell(f);
    }
  if (!free_list)
    die("Malformed host file: missing free list entry");
}

static void				/* Dump all known hosts to the host file */
write_host_db(void)
{
  DBG("Flushing host database");
  bclose(host_file);
  unlink(host_bak_name);
  if (rename(host_file_name, host_bak_name) < 0)
    die("Cannot rename %s to %s: %m", host_file_name, host_bak_name);
  host_file = bopen(host_file_name, O_RDWR|O_CREAT|O_TRUNC, 16384);
  walk_hosts(write_host_entry);
  bclose(host_file);
  unlink(host_bak_name);
}

void					/* Initialize the queueing mechanism */
queue_init(uns flat_mode)
{
  DBG("queue_init: flat_mode=%d", flat_mode);
  flat_queue_mode = flat_mode;
  host_pool = mp_new(16384);
  clist_init(&active_nodes);
  clist_init(&idle_nodes);
  clist_init(&active_hosts);
  clist_init(&idle_hosts);
  waiting_heap_init(&waiting_node_heap);
  ready_heap_init();
  host_hash = xmalloc_zero(host_hash_size * sizeof(struct qhost *));
  node_hash = xmalloc_zero(key_hash_size * sizeof(struct qnode *));
  load_host_list();
  if ((queue_fd = ucw_open(queue_file_name, O_RDWR | O_CREAT, 0666)) < 0)
    die("Unable to open queue file: %m");
  queue_cache = pgc_open(QUEUE_PAGE_SIZE, queue_cache_size);
  queue_size = ucw_seek(queue_fd, 0, SEEK_END);
}

void					/* Cleanup of the queue machinery */
queue_cleanup(void)
{
  DBG("queue_cleanup");
  write_host_db();
  pgc_close(queue_cache);
}

void					/* Write out all modified records */
queue_sync(void)
{
  DBG("queue_sync");
  pgc_flush(queue_cache);
  flush_host_cache();
  bflush(host_file);
}
