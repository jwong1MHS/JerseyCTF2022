/*
 *	Sherlock Indexer -- Image Thumbails
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_IMAGES_H
#define _SHERLOCK_INDEXER_IMAGES_H

#include "images/images.h"
#include "images/signature.h"

struct image_thumb {
  u32 id;
  u16 cols;
  u16 rows;
  u16 thumb_cols;
  u16 thumb_rows;
  struct image_vector vector;
  byte thumb_format;
  u32 thumb_size;
};

#endif
