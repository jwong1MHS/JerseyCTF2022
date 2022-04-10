/*
 *  Simple Table-Driven Stemmer: Table Format
 *
 *  (c) 2005 Martin Mares <mj@ucw.cz>
 */

struct stem_table_hdr {
  u32 magic;				/* STEM_TABLE_MAGIC */
  u32 first_lemma;			/* Offset of the first lemma record */
  u32 root;				/* Offset of the root vertex */
  byte charset[32];
};

#define STEM_TABLE_MAGIC 0x6254531e

struct stem_table_edge {
  u32 label;				/* 4 characters on this edge */
  u32 dest;				/* Offset of vertex this edge leads to */
};

struct stem_table_vertex {
  u32 parent;				/* Offset of the parent of this vertex */
  u32 parent_label;			/* Label of incoming edge */
  u32 edge_hash_size;			/* Size of the hash table; topmost 8 bits encode number of lemmata */
  struct stem_table_edge edges[0];	/* Hash table of outgoing edges */
  u32 lemmata[0];			/* Offsets of lemmata belonging to this vertex */
};

struct stem_table_lemma {
  u32 num_variants;
  u32 variants[0];			/* List of variants; the first variant is the lemma itself */
};
