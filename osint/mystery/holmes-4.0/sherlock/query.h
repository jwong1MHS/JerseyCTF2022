/*
 *	Sherlock Library -- Sherlock Query Sender
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#ifndef _SHERLOCK_QUERY_H
#define _SHERLOCK_QUERY_H

#include <setjmp.h>

struct mempool;
struct fastbuf;
struct odes;
struct oattr;

struct sh_query {
  /* Connection settings */
  char *host;			/* Search server host name */
  int port;			/* Search server port */
  u32 ip;			/* Search server IP (0 if unknown -> filled by gethostbyname automatically) */

  /* Query data (filled by sh_query_run) */
  char *query;			/* Query string */
  struct odes *reply;		/* Reply [ 'S'=status, '(H'=header, '(C'=card, '(F'=footer ] */
  char *stat;			/* Error string or reply status line */
  int code;			/* Error code or reply status code */
  struct odes *header;		/* Reply header */
  struct odes *footer;		/* Reply footer */
  struct oattr *cards;		/* Link list of returned cards */

  /* Internals */
  struct mempool *mp;		/* Query pool (flushed by sh_query_reset) */
  struct fastbuf *fb;		/* Opened socket */
  jmp_buf jmp;			/* Where to jump on error */
};

/* Initialize query structure */
void sh_query_init(struct sh_query *q, char *host, int port);

/* Clean up all internal data (safe even after a failure) */
void sh_query_cleanup(struct sh_query *q);

/* Process a given query and returns zero on success */
int sh_query_run(struct sh_query *q, char *query);

/* Free query results (usually not necessary, freed automatically before next query) */
void sh_query_reset(struct sh_query *q);

#endif
