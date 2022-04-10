/*
 *	Sherlock Shepherd Daemon -- Export Format
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_SHEPHERD_EXPORT_H
#define _SHERLOCK_GATHER_SHEPHERD_EXPORT_H

struct export_entry {
  u32 oid;
  ucw_time_t last_checked_time;	/* bit 0 indicates that the object should be sent as a header only */
  byte weight;
};

#endif
