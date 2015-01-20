#ifndef	_LEAP_STM_H_
#define	_LEAP_STM_H_

#include "set.h"

/* defines for the leap list itself */

#define NODE_SIZE 300
#define MAX_LEVEL 10

#define SENTINEL_DIFF 2	// The number to increment in order to avoid sentinel.

/* defines for the leap list's trie */
/* TRIE_KEY_NOT_FOUND should always be (NODE_SIZE) - since on valid index could be NODE_SIZE */
#define TRIE_KEY_NOT_FOUND NODE_SIZE 

typedef setkey_t trie_key_t;
/* The type trie_val_t should be able to hold all values in a node (i.e., NODE_SIZE + 1 values) */
//typedef unsigned char trie_val_t;
typedef unsigned short trie_val_t;

typedef struct item_t {
	volatile setkey_t key;
	setval_t value;
} item_t;


#endif	/* _LEAP_STM_H_ */
