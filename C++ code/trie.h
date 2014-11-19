#ifndef _TRIE_H_
#define _TRIE_H_

#ifndef TRIE_DEBUG
#define TRIE_DEBUG
#endif

#include <pthread.h>
#include "gc.h"
#include "ptst.h"
#include "set.h"
#include "leap_stm.h"

#define TRIE_INNER_ARRAY_SIZE 256

typedef struct {
	unsigned int prefix_length : 4;
	unsigned long prefix : 60;
} trie_metadata_t;

//typedef trie_val[TRIE_INNER_ARRAY_SIZE] trie_leaf;
typedef volatile trie_val_t trie_leaf16_t[16];
typedef volatile trie_val_t trie_leaf256_t[256];

typedef volatile void* trie_inner16_t[16];
typedef volatile void* trie_inner256_t[256];

/*
typedef union trie_union
{
	volatile union trie_union *nodes[TRIE_INNER_ARRAY_SIZE];
//	trie_leaf leaves[TRIE_INNER_ARRAY_SIZE];
	volatile trie_leaf_t leaves;
} trie_head_t;
*/

/* The trie data-structure */
typedef struct {
	volatile trie_metadata_t metadata;
	volatile void* head;
} trie_t;

/* Creates a new trie with the given (key, value) pair */
void trie_create_new(trie_key_t key, trie_val_t value, volatile trie_t *new_trie, ptst_t *ptst);

/* Creates a new trie from the given sorted array of items */
void trie_create_from_array(volatile trie_t *new_trie, item_t *sorted_arr, int arr_size, ptst_t *ptst);

/* Finds a given key in the trie (returns TRIE_KEY_NOT_FOUND in case the key is not part of the trie) */
trie_val_t trie_find_val(volatile trie_t *trie, trie_key_t key);

/* Destroys and frees a given trie */
void trie_destroy(volatile trie_t *trie, ptst_t *ptst);

void _init_trie_subsystem(void);

#ifdef  TRIE_DEBUG
void print_trie(volatile trie_t *trie);
#endif  /* TRIE_DEBUG */





#endif /* _TRIE_H_ */

