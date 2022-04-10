/*
 *	Sherlock Shepherd Daemon
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_SHEPHERD_MASTER_H
#define _SHERLOCK_GATHER_SHEPHERD_MASTER_H

#include <signal.h>

/* shepherd.c */

extern char *log_name;
extern uns log_connections;
extern char **error_mails;
extern char **progress_mails;
extern uns trace_level;
extern uns ctrl_port;
extern uns ctrl_listen_queue;
extern uns ctrl_timeout;
extern clist ctrl_access_list;
extern clist prepare_hooks, finish_hooks, startup_hooks;
extern uns keep_old_states, keep_interim_states, keep_exported_states;
extern uns delete_old_states;
extern uns feeder_timeout;
extern u64 work_space, min_free_space, min_bucket_reserve;
extern uns bucket_watch_period;
extern uns periodic_cleanup;

struct hook {
  cnode n;
  char *cmd;
};

extern uns opt_idle, opt_private, opt_reap, opt_keep, opt_locked;

extern volatile sig_atomic_t shutdown_requested;		/* 0=not, 1=unused, 2=quick, 3=forced */

#define TRACE(x...) do { if (trace_level) log(L_DEBUG, x); } while(0)
#define TRACE2(x...) do { if (trace_level > 1) log(L_DEBUG, x); } while(0)

void mail_log(int importance, byte *msg);	/* 0=progress, 1=error, -1=only when switching logs */
void mail_fork(void);

void check_reload(void);

/* master-states.c */

enum phase {
  M_INCOMPLETE,
  M_CLOSED,
  M_PREPARE,
  M_PLAN,
  M_REAP,
  M_CORK,
  M_CORKED,
  M_MERGE,
  M_FEEDBACK,
  M_EQUIV,
  M_SELECT,
  M_RECORD,
  M_SORT,
  M_FINISH,
  M_CLEANUP,
  M_RECOVER,
  M_ROLLBACK,
  M_BORROWED,
  M_MAX
};

struct state {
  cnode n;
  enum phase phase;
  int num_locks;
  int export_count;
  int already_sent_report;
  int is_initial;				/* Initial state, which should be preserved if opt_keep is set */
  struct hook *current_hook;
  byte *fn;
  byte name[1];
};

extern clist state_list;
extern const byte *phase_names[];
extern uns scheduled_cleanup;
extern byte *fn_feedback, *fn_feedback_run, *fn_feedback_old;

void states_init(void);
void states_reload(void);
void states_cleanup(void);
void states_scan(void);
void states_list(void);
void states_look(void);
void states_kick(void);
int states_check(void);
void run_startup_hooks(void);
byte *state_fn(struct state *st, byte *file);
void states_rethink(void);
char *states_do_scan_single(struct state *st);
void state_delete(struct state *st, int delete_files);
void format_status(byte *p);

/* master-ctrl.c */

extern uns borrow_requested;

void ctrl_init(void);
void ctrl_reload(void);
void ctrl_borrow_ack(void);

#endif
