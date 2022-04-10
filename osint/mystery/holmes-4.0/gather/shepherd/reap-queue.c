/*
 *	Sherlock Shepherd Reaper -- Queue Management
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/heap.h"
#include "ucw/hashfunc.h"
#include "filter/filter.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"

#include <string.h>

/*
 *  The data structures are somewhat complicated due to our requirements
 *  for per-site queueing as opposed to per-server timing.
 *
 *  We keep a set of qnodes identified by so called qkeys, which are 64-bit
 *  numbers consisting of the IP address of the server (an skey), its port
 *  number and a channel ID (most servers have only a single channel, but
 *  if we want to allow simultaneous connections, there can be multiple
 *  channels). Each qnode then contains a linked list of sites. No two active
 *  sites can share a qnode. Timings are defined on qnodes, item queues are
 *  connected to sites.
 *
 *  qnode states:
 *     -  QS_IDLE (has no sites; linked in idle_nodes list)
 *     -  QS_ACTIVE (linked in active_nodes list)
 *     -  QS_WAITING (stored in waiting_node_heap)
 *     -  QS_READY (stored in ready_node_heap)
 *
 *  site states:
 *     -  SS_IDLE (no items queued; linked in idle_sites list)
 *     -  SS_ACTIVE (linked in active_sites list), the qkey can be altered now
 *     -  SS_WAITING (linked in its waiting or ready qnode's heap)
 *
 *  One of the sites (pointed to by free_list) serves as dummy header
 *  of the list of free queue file blocks. It's always kept in the
 *  idle_sites list.
 *
 *  Some queue keys are reserved for special cases (see also SKEY_* in shepherd.h):
 *     00000000 - 0000ffff	Sites waiting for resolving of real key
 *     7f010000 - 7f01ffff	Non-IP sites (file:// etc.)
 *     7f020000 - 7f02ffff	Keys for unresolvable sites
 *  Bottom 16 bits are generated randomly to allow resolving to run in parallel.
 *  These nodes take precedence over normal nodes (this is achived by their wait time being
 *  always zero), but normal nodes cannot starve as the number of these nodes is limited
 *  by max_resolvers and max_flushers.
 */

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
static uns ready_heap_max;
static uns ready_heap_n;

#define RHEAP_LESS(x,y) ((x)->qpriority > (y)->qpriority || ((x)->qpriority == (y)->qpriority && (x)->sequence > (y)->sequence))
#define RHEAP_SWAP(h,i,j,t) do { t=h[i]; h[i]=h[j]; h[j]=t; h[i]->link.heap_index=i; h[j]->link.heap_index=j; } while (0)

static void
ready_heap_init(void)
{
  ready_heap_n = 0;
  ready_heap_max = 16;
  ready_heap = xmalloc(sizeof(struct qnode *) * (ready_heap_max+1));
}

static void
ready_heap_insert(struct qnode *n)
{
  if (ready_heap_n >= ready_heap_max)
    {
      ready_heap_max = 2*ready_heap_max;
      DBG("QUEUE: Re-allocating ready heap to %d entries", ready_heap_max);
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

uns
queue_ready_count(void)
{
  return ready_heap_n;
}

/* The heap of sites in each queue node: ordered by priority and then by sequence numbers */

#define BH_PREFIX(x) site_heap_##x
#define BH_WANT_INSERT
#define BH_WANT_FINDMIN
#define BH_WANT_DELETEMIN
static inline int site_heap_less(struct bh_node *a, struct bh_node *b)
{
  struct qsite *x = (struct qsite *) a;
  struct qsite *y = (struct qsite *) b;
  COMPARE_GT(x->qpriority, y->qpriority);
  return (x->sequence < x->sequence);
}
#include "ucw/binheap.h"

static clist idle_nodes;		/* Queue nodes in state QS_IDLE */
static clist active_nodes;		/* Queue nodes in state QS_ACTIVE */
static clist idle_sites;		/* Sites in state SS_IDLE */
static clist active_sites;		/* Sites in state SS_ACTIVE */

static struct mempool *qpool;
static uns sequence_counter;

#define HASH_NODE struct qnode
#define HASH_PREFIX(x) qnode_##x
#define HASH_KEY_ATOMIC qkey
#define HASH_ATOMIC_TYPE u64
#define HASH_WANT_LOOKUP
#define HASH_USE_POOL qpool
#define HASH_ZERO_FILL
#define HASH_GIVE_INIT_DATA

static void
qnode_init_data(struct qnode *n)
{
  n->state = QS_IDLE;
  n->std_delay = std_server_delay;
  clist_add_tail(&idle_nodes, &n->link.list_node);
  site_heap_init(&n->site_heap);
}

#include "ucw/hashtable.h"

struct qsite *
get_site(struct qnode **pnode)
{
  /*
   *  Get the best of ready sites and remove it (and its qnode) from the queue.
   */

  struct qsite *h;
  struct qnode *n;

  n = (struct qnode *) ready_heap_deletemin();
  if (!n)
    {
      DBG("QUEUE: No sites are ready");
      return NULL;
    }
  DBG("QUEUE: Dequeued qkey %04x:%08x:%d pri %d", QK_TRIPLE(n->qkey), n->qpriority);
  ASSERT(n->state == QS_READY);
  n->state = QS_ACTIVE;
  clist_add_tail(&active_nodes, &n->link.list_node);

  h = (struct qsite *) site_heap_deletemin(&n->site_heap);
  ASSERT(h && h->state == SS_WAITING);
  h->state = SS_ACTIVE;
  n->active_site = h;
  clist_add_tail(&active_sites, &h->link.list_node);
  DBG("QUEUE: Dequeued site %p pri %d", h, h->qpriority);

  *pnode = n;
  return h;
}

static uns
calc_delay(struct qnode *n)
{
  if ((n->qkey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED)		/* Resolves are not delayed */
    return 0;
  if (n->last_was_autoreply)					/* Autoreplies (including flushes) have their own delay */
    return autoreply_delay;

  uns dly = n->std_delay;
  if (n->conn_err_count)
    dly = MAX(dly, conn_err_delay);
  if (dly > server_overtake)
    dly -= server_overtake;
  return dly;
}

static void
relink_qnode(struct qsite *h, int insert_site)
{
  /*
   *  Relink the qnode corresponding to a given site to the appropriate list / heap.
   *  If insert_site is set, insert the site to the node's waiting heap.
   */

  struct qnode *n = h->qnode;
  DBG("\tQkey %04x:%08x:%d in state %d, our site pri %d seq %d", QK_TRIPLE(n->qkey), n->state, h->qpriority, h->sequence);

  /* Insert our site to the node's heap */
  if (insert_site)
    site_heap_insert(&n->site_heap, &h->link.heap_node);

  /* Unlink the node or decide it will remain as it is */
  switch (n->state)
    {
    case QS_ACTIVE:
      if (n->active_site != h)
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
  struct qsite *best = (struct qsite *) site_heap_findmin(&n->site_heap);
  if (!best)
    {
      ASSERT(!insert_site);
      n->state = QS_IDLE;
      clist_add_tail(&idle_nodes, &n->link.list_node);
      DBG("\tQS_IDLE");
      return;
    }
  else if ((n->qkey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED ||
	   (n->qkey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT)	/* Always prefer resolvers and nonexistent hosts */
    n->qpriority = ~0U;
  else
    n->qpriority = best->qpriority;
  n->sequence = sequence_counter++;

  /* Enqueue us according to the timeout to either ready or waiting qnodes */
  uns delay = calc_delay(n);
  if (!n->last_access)
    n->wait_until = main_now_seconds;
  else
    n->wait_until = n->last_access + delay;
  if (n->wait_until > main_now_seconds)
    {
      n->state = QS_WAITING;
      waiting_heap_insert(&waiting_node_heap, &n->link.heap_node);
      DBG("\tQS_WAITING for %d sec with pri %d", (int)(n->wait_until - main_now_seconds), n->qpriority);
    }
  else
    {
      n->state = QS_READY;
      ready_heap_insert(n);
      DBG("\tQS_READY with pri %d", n->qpriority);
    }
}

void
put_site(struct qsite *h)
{
  /*
   *  Take a site previously returned by get_site() and put it
   *  back to the queue, possibly updating its queue key and
   *  priority.
   */

  DBG("QUEUE: Putting site %p, state=%d, new_skey=%08x", h, h->state, h->new_skey);
  ASSERT(h->state == SS_ACTIVE);

  /* First of all, unlink the site node from its original place */
  clist_remove(&h->link.list_node);

  /* If the skey has changed, we need to relink the original qnode */
  if (h->new_skey && h->new_skey != qkey_to_skey(h->qnode->qkey))
    {
      relink_qnode(h, 0);
      /* At this point, we always use channel 0 in the new qkey */
      h->qnode = qnode_lookup(make_qkey(h->new_skey, qkey_to_port(h->qnode->qkey)));
      h->new_skey = 0;
    }

  /* Check if we have anything to do */
  if (h->plan_start >= h->plan_end)
    {					/* Idle site */
      h->state = SS_IDLE;
      relink_qnode(h, 0);
      clist_add_tail(&idle_sites, &h->link.list_node);
    }
  else
    {
      h->qpriority = h->plan_start[0].priority;
      h->state = SS_WAITING;
      relink_qnode(h, 1);
    }
}

static void
update_site(struct qsite *h)
{
  /*
   *  Should be called whenever the plan for a site is updated to ensure
   *  consistency of queue data structures.
   */

  DBG("QUEUE: Updating site %p, state=%d", h, h->state);
  if (h->state == SS_IDLE && h->plan_start < h->plan_end)	/* The site need activation */
    {
      h->qpriority = h->plan_start[0].priority;
      clist_remove(&h->link.list_node);
      h->state = SS_WAITING;
      relink_qnode(h, 1);
    }
}

int
queue_time_step(ucw_time_t *p_wait_seconds)
{
  /*
   *  Called occassionally to remind the queue about the ephemerality
   *  of the Universe, or, to be more precise and less philosophical,
   *  to tell the queue manager that some time has passed and that it
   *  should reconsider which nodes are waiting and which are al-ready.
   *  Also, it returns when will the next waiting node wake up and
   *  whether there are any ready, running or waiting nodes.
   */

  struct qnode *q;

  DBG("QUEUE: The time has passed: now=%d", (int)main_now_seconds);
  while (q = (struct qnode *) waiting_heap_findmin(&waiting_node_heap))
    {
      if (q->wait_until > main_now_seconds)
	{
	  DBG("\tWaiting until %d", (int)q->wait_until);
	  *p_wait_seconds = q->wait_until - main_now_seconds;
	  return 1;
	}
      waiting_heap_deletemin(&waiting_node_heap);
      DBG("\tWoken up qkey %04x:%08x:%d", QK_TRIPLE(q->qkey));
      ASSERT(q->state == QS_WAITING);
      q->state = QS_READY;
      ready_heap_insert(q);
    }
  DBG("\tNo more waiting nodes.");
  if (ready_heap_n || !clist_empty(&active_nodes))
    {
      DBG("\tBut some ready or running ones.");
      return 1;
    }
  return 0;
}

static void		       /* Initialize the queueing mechanism */
queue_init(void)
{
  DBG("QUEUE: Initializing");
  qpool = mp_new(16384);
  clist_init(&active_nodes);
  clist_init(&idle_nodes);
  clist_init(&active_sites);
  clist_init(&idle_sites);
  waiting_heap_init(&waiting_node_heap);
  ready_heap_init();
  qnode_init();
}

uns initial_plan_entry_count;
uns current_plan_entry_count;

void
load_plan(void)
{
  uns plan_len;
  struct plan_site_entry *plan, *p, *plan_end;
  uns site_count = 0;

  plan = mmap_file(state_file_name(current_state, "plan"), &plan_len, 0);
  plan_end = (void *)((byte *)plan + plan_len);
  queue_init();

  for (p=plan; p<plan_end;)
    {
      struct qsite *h = mp_alloc_zero(qpool, sizeof(struct qsite));
      h->robot_id = p->robot_oid;
      h->qnode = qnode_lookup(p->qkey);
      h->plan_start = (struct plan_entry *)(p+1);
      h->plan_end = h->plan_start + p->entry_count;
      h->state = SS_IDLE;
      clist_add_tail(&idle_sites, &h->link.list_node);
      update_site(h);
      struct qnode *n = h->qnode;
      n->std_delay = p->delay;
      site_count++;
      initial_plan_entry_count += p->entry_count;
      p = (struct plan_site_entry *) h->plan_end;
    }
  log(L_INFO, "Loaded plan for %d sites: %d entries, %d queue keys", site_count, initial_plan_entry_count, qnode_table.hash_count);
  current_plan_entry_count = initial_plan_entry_count;
}

struct plan_entry *
plan_get_next(struct qsite *s)
{
  ASSERT(s->plan_start < s->plan_end);
  current_plan_entry_count--;
  return s->plan_start++;
}

void
plan_unget_next(struct qsite *s)
{
  current_plan_entry_count++;
  s->plan_start--;
}

void
plan_reset_site(struct qsite *s)
{
  current_plan_entry_count -= s->plan_end - s->plan_start;
  s->plan_start = s->plan_end;
}

uns
plan_count_qkeys(void)
{
  uns qkeys = 0;

  HASH_FOR_ALL(qnode, q)
    {
      if (q->state != QS_IDLE)
	qkeys++;
    }
  HASH_END_FOR;
  return qkeys;
}
