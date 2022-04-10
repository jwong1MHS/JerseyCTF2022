/*
 * 	Sherlock Shepherd Daemon -- Communication Protocol
 *
 * 	(c) 2004--2005 Martin Mares <mj@ucw.cz>
 * 	(c) 2005 Robert Spalek <robert@ucw.cz>
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_SHEPHERD_PROTOCOL_H
#define _SHERLOCK_GATHER_SHEPHERD_PROTOCOL_H

#define SHEPHERD_DEFAULT_PORT 8187

struct shepp_packet_hdr {
  u32 leader;			// Always SHEPP_LEADER
  u32 type;			// SHEPP_REQ_xxx or SHEPP_REPLY_xxx
  u32 id;			// Packet ID for matching of replies with requests
  u32 data_len;			// Length of the data part
} PACKED;

/*
 *  Request and reply types.
 *  Encoding: 0x0000MPcc, where `M' encodes mode, `P' payload type, and `cc' is command code.
 */

enum shepp_request_payload {
  SHEPP_PAYLOAD_NONE  	     = 0x0000,	// Empty
  SHEPP_PAYLOAD_RAW   	     = 0x0100,	// Raw data
  SHEPP_PAYLOAD_ATTRS	     = 0x0200,	// Attributes in BUCKET_TYPE_V33 format
};

enum shepp_request_type {		// Request types available in normal mode
  SHEPP_REQ_SEND_MODE	     = 0x1200,	// Lock last closed state and switch to send mode (or SHEPP_REPLY_DEFER)
  SHEPP_REQ_PING      	     = 0x1001,	// Send SHEPP_REPLY_PONG
  SHEPP_REQ_SET_CLEANUP	     = 0x1102,	// Set --cleanup switch (payload: u32 value)
  SHEPP_REQ_SET_IDLE	     = 0x1103,	// Set --idle switch (payload: u32 value)
  SHEPP_REQ_SET_PRIVATE	     = 0x1104,	// Set --private switch (payload: u32 value)
  SHEPP_REQ_LOCK_STATE	     = 0x1005,	// Lock last closed state for the duration of this connection
  SHEPP_REQ_BORROW_STATE     = 0x1006,	// Borrow the current state for manual changes when it becomes closed
  SHEPP_REQ_BORROW_STATE_Q   = 0x1007,	// Borrow the current state when it becomes closed or corked
  SHEPP_REQ_RETURN_STATE     = 0x1008,	// Return the borrowed state and continue processing
  SHEPP_REQ_ROLLBACK_STATE   = 0x1009,	// Discard the borrowed state and roll back to last closed state
  SHEPP_REQ_SET_DELETE_OLD   = 0x110a,	// Set ShepMaster.DeleteOldStates (payload: u32 value)
  SHEPP_REQ_UNLOCK_STATES    = 0x100b,	// Reset --locked switch
};

enum shepp_send_request_type {		// Request types available in send mode
  SHEPP_REQ_SEND_BUCKETS_V32 = 0x2000,	// OBSOLETE
  SHEPP_REQ_SEND_FEEDBACK    = 0x2001,	// Upload indexer feedback (DATA_BLOCK* DATA_END follows)
  SHEPP_REQ_SEND_DATA_BLOCK  = 0x2102,	// Raw data upload
  SHEPP_REQ_SEND_DATA_END    = 0x2003,	// End of raw data upload
  SHEPP_REQ_SEND_RAW_BUCKETS = 0x2204,	// Like SEND_BUCKETS, but send raw data only
  SHEPP_REQ_SEND_RAW_INDEX   = 0x2205,	// Send the state index file in raw format (reply: DATA_BLOCK* DATA_END)
  SHEPP_REQ_SEND_BUCKETS     = 0x2206,	// Send contents of all buckets to the indexer (reply: DATA_BLOCK* DATA_END)
					// Each bucket starts with struct shepp_bucket_header
  SHEPP_REQ_SEND_RAW_SITES   = 0x2207,	// Send the site file in raw format (reply: DATA_BLOCK* DATA_END)
  SHEPP_REQ_SEND_RAW_PARAMS  = 0x2208,	// Send the parameters in raw format (reply: DATA_BLOCK* DATA_END)
  SHEPP_REQ_SEND_URLS        = 0x2109,	// Send the URL database (reply: DATA_BLOCK* DATA_END)
					// (payload: u64 offset for incremental sending, ~0 means header only)
  SHEPP_REQ_SEND_BUCKET      = 0x210a,	// Send a given bucket (reply: DATA_BLOCK* DATA_END) (payload: footprint)
};

enum shepp_reply_type {
  SHEPP_REPLY_OK	     = 0x0000,	// Request succeeded
  SHEPP_REPLY_UNKNOWN_REQ    = 0x0001,	// Unknown request received
  SHEPP_REPLY_NOT_AUTHORIZED = 0x0002,	// Unauthorized connection or command
  SHEPP_REPLY_WELCOME	     = 0x0203,	// Sent when successfully connected
  SHEPP_REPLY_PONG	     = 0x0004,	// Ping reply
  SHEPP_REPLY_SEND_MODE	     = 0x0205,	// Switched to send mode, sending state parameters
  SHEPP_REPLY_DEFER	     = 0x0206,	// Operation refused because of pending cleanup, try again later
  SHEPP_REPLY_DATA_BLOCK     = 0x0107,	// Raw data download
  SHEPP_REPLY_DATA_END	     = 0x0008,	// End of raw data download
  SHEPP_REPLY_IN_PROGRESS    = 0x0009,	// Operation already in progress
  SHEPP_REPLY_NO_BORROWED    = 0x000a,	// No borrowed state exists
  SHEPP_REPLY_RETURNING_BAD  = 0x000b,	// Trying to return an inconsistent state
  SHEPP_REPLY_NO_SUCH_STATE  = 0x000c,	// State of a given name doesn't exist
  // the following codes are internal for protocol.c:
  SHEPP_REPLY_TIMEOUT	     = 1001,	// Connection timed out
  SHEPP_REPLY_INTERNAL	     = 1002,	// Internal error of the communication protocol
};

#define SHEPP_LEADER 0x27182818

/*
 *  List of attributes sent in various packets:
 *
 *  SHEPP_REPLY_WELCOME:
 *	Vversion		Protocol version (this version is 330)
 *
 *  SHEPP_REQ_SEND_MODE:
 *	Mmax			Send at most this number of buckets
 *	Wweight			Send only buckets with this weight or more
 *	Bbest-active		Select this number of best active objects
 *	bbest-inactive		Select this number of best inactive objects
 *	Sname			Request a specific state to be sent
 *
 *  SHEPP_REPLY_SEND_MODE:
 *	Nobjects		Number of objects
 *	Sstate			State name
 */

struct shepp_bucket_header {
  u32 oid;
  u32 length;
  u32 type;
};

/* Library functions in protocol.c */

extern int shepp_fd;
extern u32 shepp_id_counter;
extern int shepp_timeout;
extern int shepp_version;

struct odes;

extern void (*shepp_error_cb)(uns code, char *msg); // Callback
void NONRET shepp_error(uns code, char *msg);
void NONRET shepp_err(char *msg, ...);
void shepp_read(void *buf, uns size);
void shepp_skip(uns size);
void shepp_write(void *buf, uns size);
struct odes *shepp_new_attrs(void);
void shepp_send_hdr(struct shepp_packet_hdr *pkt, struct shepp_packet_hdr *reply_to);
void shepp_send_none(struct shepp_packet_hdr *pkt, u32 type, struct shepp_packet_hdr *reply_to);
void shepp_send_raw(struct shepp_packet_hdr *pkt, u32 type, struct shepp_packet_hdr *reply_to, void *data, uns len);
void shepp_send_attrs(struct shepp_packet_hdr *pkt, u32 type, struct shepp_packet_hdr *reply_to, struct odes *obj);
int shepp_recv_hdr(struct shepp_packet_hdr *pkt, struct shepp_packet_hdr *reply_to);
void *shepp_recv_data(struct shepp_packet_hdr *pkt);
void *shepp_recv(struct shepp_packet_hdr *pkt, struct shepp_packet_hdr *reply_to);
void shepp_unex(struct shepp_packet_hdr *pkt);
byte **shepp_connect(byte *name);
struct odes *shepp_send_mode(byte **options);
byte *shepp_encode_attrs(struct odes *attrs, u32 *lenp);
void shepp_decode_attrs(struct odes *attrs, byte *data, uns len);

/* shep_connect() returns optional parameters plus host:port at the following indices: */
#define SHEP_CONNECT_HOST	-2
#define SHEP_CONNECT_PORT	-1

/*
 *  protocol.c uses a shared memory pool for all packet data and attributes.
 *  shepp_new_attrs flushes the pool and stores the new struct odes there.
 *  shepp_recv_* also flush the pool and store received data and attributes in it.
 *  Therefore all allocated data are local to a single protocol transaction.
 */

/* protocol-fb.c */

struct fastbuf *shepp_fb_open_read(struct shepp_packet_hdr *reply_to);
struct fastbuf *shepp_fb_open_write(struct shepp_packet_hdr *reply_to);

#endif
