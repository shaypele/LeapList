#include "trie.h"
#include <string.h> /* for memcpy */
#include <strings.h> /* for bzero */
#include <stdio.h>
#include <pthread.h> /* for ptst */
#include <stdlib.h>

#define LAST_2_DIGITS_MASK 0xff
#define LAST_DIGIT_MASK 0xf
#define TRIE_KEY_MAX_DIGITS 16

#define ASSERT_GC(X) {if(X==0) exit(999);}

static int gc_id_inner_node256;
static int gc_id_inner_node16;
static int gc_id_leaves256;
static int gc_id_leaves16;

void _init_trie_subsystem(void)
{
	gc_id_inner_node256 = gc_add_allocator(sizeof(trie_inner256_t));
	gc_id_inner_node16 = gc_add_allocator(sizeof(trie_inner16_t));
	gc_id_leaves256 = gc_add_allocator(sizeof(trie_leaf256_t));
	gc_id_leaves16 = gc_add_allocator(sizeof(trie_leaf16_t));
}

/* Allocates a single trie node from the memory */
static volatile void** allocate_trie_inner_node256(ptst_t *ptst)
{
	volatile void **new_node;

	new_node = (volatile void **) gc_alloc(ptst, gc_id_inner_node256);
	ASSERT_GC(new_node);

	return new_node;
}

static volatile void** allocate_trie_inner_node16(ptst_t *ptst)
{
	volatile void **new_node;

	new_node = (volatile void **) gc_alloc(ptst, gc_id_inner_node16);
	ASSERT_GC(new_node);

	return new_node;
}


/* Allocates a single trie leaf node from the memory */
static volatile trie_val_t* allocate_trie_leaves_node256(ptst_t *ptst)
{
	volatile trie_val_t *new_node;

	new_node = (volatile trie_val_t *) gc_alloc(ptst, gc_id_leaves256);
	ASSERT_GC(new_node);

	return new_node;
}

static volatile trie_val_t* allocate_trie_leaves_node16(ptst_t *ptst)
{
	volatile trie_val_t *new_node;

	new_node = (volatile trie_val_t *) gc_alloc(ptst, gc_id_leaves16);
	ASSERT_GC(new_node);

	return new_node;
}

/* Deallocates a single trie node from the memory */
static void free_trie_inner_node256(volatile void** node, ptst_t * ptst)
{
	gc_free(ptst, node, gc_id_inner_node256);
}

static void free_trie_inner_node16(volatile void** node, ptst_t * ptst)
{
	gc_free(ptst, node, gc_id_inner_node16);
}

/* Deallocates a single trie node from the memory */
static void free_trie_leaves_node256(volatile trie_val_t* node, ptst_t * ptst)
{
	gc_free(ptst, node, gc_id_leaves256);
}

static void free_trie_leaves_node16(volatile trie_val_t* node, ptst_t * ptst)
{
	gc_free(ptst, node, gc_id_leaves16);
}




/* Resets the leaves to TRIE_KEY_NOT_FOUND */
/*
#define RESET_LEAVES(arr, elements) {			\
		int l;					\
		for (l = 0; l < elements; l++)		\
			arr[l] = TRIE_KEY_NOT_FOUND;	\
	}
*/
#define RESET_LEAVES(arr, elements)	bzero(arr, sizeof(trie_val_t)*elements)
/* RESET_INNER256 - nulls 256 pointers (of size 8 byte) */
#define RESET_INNER256(arr)	bzero(arr, 2048)
/* RESET_INNER16 - nulls 16 pointers (of size 8 byte) */
#define RESET_INNER16(arr)	bzero(arr, 128)



/* Creates a new trie with the given (key, value) pair */
void trie_create_new(trie_key_t key, trie_val_t value, volatile trie_t *new_trie, ptst_t *ptst)
{
	int i = 0;

	/* Creates a trie with only leaves */
	volatile trie_val_t *new_node = allocate_trie_leaves_node16(ptst);
	new_trie->head = new_node;
	RESET_LEAVES(new_node, 16);

	new_trie->metadata.prefix_length = TRIE_KEY_MAX_DIGITS - 1;
	new_trie->metadata.prefix = (key >> 4);
	new_node[key & LAST_DIGIT_MASK] = (value + 1);

}



void compute_trie_prefix(volatile trie_t *trie, trie_key_t low, trie_key_t high)
{
	int i;
	for (i = 1; i <= TRIE_KEY_MAX_DIGITS; i++)
	{
		if ((low >> (i << 2)) == (high >> (i << 2)))
		{
			trie->metadata.prefix_length = TRIE_KEY_MAX_DIGITS - i;
			trie->metadata.prefix = (low >> (i << 2));
			return;
		}
	}

}



/* Creates a new trie which is based on the given array.
   Assumes the array has at least one element. */
void trie_create_from_array(volatile trie_t *new_trie, item_t *sorted_arr, int arr_size, ptst_t *ptst)
{
	int i;
	trie_val_t counter = 1; /* Starts from 1 because we 0 is the key-not-found value */

	/* First compute the prefix */
	compute_trie_prefix(new_trie, sorted_arr[0].key, sorted_arr[arr_size - 1].key);

	unsigned long new_prefix = new_trie->metadata.prefix;
//	printf("NEW TRIE! prefix_length = %d; prefix = %lx\n", new_trie->metadata.prefix_length, new_prefix);

	/* initialize the trie contents */

	/* In case the trie has only leaves */
	if (new_trie->metadata.prefix_length == 14)
	{
		/* The trie nedds to be with only 256 leaves */
		volatile trie_val_t *new_node = allocate_trie_leaves_node256(ptst);
		new_trie->head = new_node;
		RESET_LEAVES(new_node, 256);

		for (i = 0; i < arr_size; i++)
		{
			int cur_key_index = (sorted_arr[i].key & LAST_2_DIGITS_MASK);
			new_node[cur_key_index] = counter;
			counter++;
		}
	}
	else if (new_trie->metadata.prefix_length == 15)
	{
		/* The trie nedds to be with only 16 leaves */
		volatile trie_val_t *new_node = allocate_trie_leaves_node16(ptst);
//		printf("\t\t Cur head is at %p\n", new_trie->head);
		new_trie->head = new_node;
//		printf("\t\t New head is at %p\n", new_trie->head);
		RESET_LEAVES(new_node, 16);

		for (i = 0; i < arr_size; i++)
		{
			int cur_key_index = (sorted_arr[i].key & LAST_DIGIT_MASK);
//			printf("\t\t Adding value %d to index %d\n", counter, cur_key_index);
			new_node[cur_key_index] = counter;
			counter++;
		}

	}
	else
	{
		/* In case the trie has inner nodes */



		if (new_trie->metadata.prefix_length % 2 == 0)
		{
			/* The trie needs to be with 256 inner node */
			volatile void **new_node = allocate_trie_inner_node256(ptst);
			new_trie->head = (volatile void *)new_node;
			RESET_INNER256(new_node);
/*
			for (i = 0; i < 256; i++)
				new_node[i] = NULL;
*/

		}
		else
		{
			/* The trie needs to be with 16 inner node */
			volatile void **new_node = allocate_trie_inner_node16(ptst);
			new_trie->head = (volatile void *)new_node;
			RESET_INNER16(new_node);
/*
			for (i = 0; i < 16; i++)
				new_node[i] = NULL;
*/

		}

		


//		printf("Here1\n");
		/* Add each key from the array (if the inner nodes are not there, create them - their size is 256) */
		for (i = 0; i < arr_size; i++)
		{

			int shift_digits = (TRIE_KEY_MAX_DIGITS - new_trie->metadata.prefix_length);
			int array_index;
			volatile void **cur_node = (volatile void **)new_trie->head;
			volatile trie_val_t *leaf_node = (volatile trie_val_t *)new_trie->head;
			trie_key_t cur_key = sorted_arr[i].key;

//			printf("Here2 - adding key %lx\n", cur_key);

			if (shift_digits % 2 == 0)
				array_index = (cur_key >> ((shift_digits - 2) << 2)) & LAST_2_DIGITS_MASK;
			else
			{
				array_index = (cur_key >> ((shift_digits - 1) << 2)) & LAST_DIGIT_MASK;
				shift_digits++; /* make shift_digits an even number */
			}

			/* Traverse the inner nodes in the trie */
			while (shift_digits > 2)
			{
//				printf("Checking if need to allocate \n");
				if (cur_node[array_index] == NULL)
				{
					if (shift_digits <= 4)
					{
//						printf("need to allocate leaves\n");
						/* the node will hold leaves */
						volatile trie_val_t *new_node = allocate_trie_leaves_node256(ptst);
//						printf("finished allocation - new leaves address %p\n", new_node);
						RESET_LEAVES(new_node, 256);
//						printf("finished resetting\n");
						cur_node[array_index] = new_node;
//						printf("cur_node set in array_index %x, and address of new_node is: %p\n", array_index, new_node);
					}
					else
					{
						int j;
//						printf("need to allocate inner node\n");
						/* Inner node */
						volatile void **new_node = allocate_trie_inner_node256(ptst);
//						printf("finished allocation - new inner node address %p\n", new_node);
						//bzero(new_node->nodes, sizeof(trie_head_t));
						RESET_INNER256(new_node);
/*
						for (j = 0; j < 256; j++)
							new_node[j] = NULL;
*/
//						printf("finished resetting\n");
						cur_node[array_index] = new_node;
//						printf("cur_node set in array_index %x\n", array_index);
					}
				}

				cur_node = cur_node[array_index];
				leaf_node = (trie_val_t *)cur_node;
				shift_digits = shift_digits - 2;
				array_index = (cur_key >> ((shift_digits - 2) << 2)) & LAST_2_DIGITS_MASK;
			}

			/* Add the key-val to a leaf */
//			printf("\t\t Cur head is at %p\n", new_trie->head);
//			printf("\t\t Cur leaf node is at %p\n", leaf_node);
//			printf("leaf array_index %x\n", array_index);
			leaf_node[array_index] = counter;


			counter++;


		}

	}
	
/*
	if (counter != arr_size)
	{
		printf("Error when adding - counter is %d, and should be %d\n", counter, arr_size);
		printf("Trie:\n");
		print_trie(new_trie);
		printf("keys:\n");
		for (i = 0; i < arr_size; i++)
			printf("%lx, ", sorted_arr[i]);
		printf("\n");
		exit(18);
	}
*/
}






/* Finds a given key in the trie (returns TRIE_KEY_NOT_FOUND in case the key is not part of the trie) */
trie_val_t trie_find_val(volatile trie_t *trie, trie_key_t key)
{
	int shift_digits = (TRIE_KEY_MAX_DIGITS - trie->metadata.prefix_length);
	int array_index;
	volatile void **cur_node;
	volatile trie_val_t *leaf_node;
	trie_val_t ret;

	/* If the trie is empty, return */
	if (trie->head == NULL)
		return TRIE_KEY_NOT_FOUND;

	/* Check the base case (if the key does not match the prefix) */
	if ((key >> (shift_digits << 2)) != trie->metadata.prefix)
		return TRIE_KEY_NOT_FOUND;

//	printf("Trying to find key: %lx\n", key);
//	print_trie(trie);

	cur_node = (void **)trie->head;
	leaf_node = (trie_val_t *)trie->head;

	if (shift_digits % 2 == 0)
		array_index = (key >> ((shift_digits - 2) << 2)) & LAST_2_DIGITS_MASK;
	else
	{
		array_index = (key >> ((shift_digits - 1) << 2)) & LAST_DIGIT_MASK;
		shift_digits++; /* make shift_digits an even number */
	}

	/* Traverse the inner nodes in the trie */
	while (shift_digits > 2)
	{
		if (cur_node[array_index] == NULL)
			return TRIE_KEY_NOT_FOUND;

		cur_node = cur_node[array_index];
		leaf_node = (trie_val_t *)cur_node;
		shift_digits = shift_digits - 2;
		array_index = (key >> ((shift_digits - 2) << 2)) & LAST_2_DIGITS_MASK;
	}

	/* Reached a leaf */
//	printf("key: %lx found at (last_arry_index %d)\n", key, array_index);
	
	ret = leaf_node[array_index];
	if (ret == 0)
		return TRIE_KEY_NOT_FOUND;
		
	return ret - 1;
}





static void rec_trie_destroy(volatile void *node, int cur_level, ptst_t *ptst)
{
	if (cur_level == 7)
	{
		/* It is a 256 leaf node, destroying it... */
		free_trie_leaves_node256((volatile trie_val_t *)node, ptst);
	}
	else
	{
		/* Reached an inner node with 256 elements, need to recursively destroy each of them, and then my own node */
		volatile void **real_node = (volatile void **)node;
		int i;
		for (i = 0; i < 256; i++)
		{
			if (real_node[i] != NULL)
			{
				rec_trie_destroy(real_node[i], cur_level + 1, ptst);
				//real_node[i] = NULL;
			}
		}
		free_trie_inner_node256(real_node, ptst);
	}
}

/* Destroys a given trie and deallocates its memory */
void trie_destroy(volatile trie_t *trie, ptst_t *ptst)
{


	/* If the trie is empty, return */
	if (trie->head == NULL)
		return;


	if (trie->metadata.prefix_length == 14)
	{
		/* The trie is with only 256 leaves */
		free_trie_leaves_node256((trie_val_t *)trie->head, ptst);
	}
	else if (trie->metadata.prefix_length == 15)
	{
		/* The trie is with only 16 leaves */
		free_trie_leaves_node16((trie_val_t *)trie->head, ptst);
	}
	else
	{
		int elements_num = ((trie->metadata.prefix_length % 2 == 0) ? 256 : 16);
		volatile void **real_head = (volatile void **)trie->head;
		int i;
		for (i = 0; i < elements_num; i++)
		{
			if (real_head[i] != NULL)
			{
				rec_trie_destroy(real_head[i], (trie->metadata.prefix_length / 2) + 1, ptst);
				//real_head[i] = NULL;
			}
		}
		if (elements_num == 256)
			free_trie_inner_node256(real_head, ptst);
		else
			free_trie_inner_node16(real_head, ptst);

	}

//	printf("End- Destroying a trie - trie's level %d\n", (trie->metadata.prefix_length / 2));

/*
	trie->head = NULL;
	trie->metadata.prefix_length = 0;
	trie->metadata.prefix = 0;
*/
}









#ifdef	TO_TEST
static int rec_trie_nodes_num(volatile void *node, int cur_level)
{
	if (cur_level == 7)
	{
		/* It is a 256 leaf node, destroying it... */
		return 1;
	}
	else
	{
		/* Reached an inner node with 256 elements, need to recursively destroy each of them, and then my own node */
		volatile void **real_node = (volatile void **)node;
		int sum = 0;
		int i;
		for (i = 0; i < 256; i++)
		{
			if (real_node[i] != NULL)
			{
				sum = sum + rec_trie_nodes_num(real_node[i], cur_level + 1);
			}
		}
		sum = sum + 1;
		return sum;
	}
}
static int trie_nodes_num(volatile trie_t *trie)
{
	/* If the trie is empty, return */
	if (trie->head == NULL)
		return 0;


	if (trie->metadata.prefix_length >= 14)
	{
		return 1;
	}
	else
	{
		int elements_num = ((trie->metadata.prefix_length % 2 == 0) ? 256 : 16);
		volatile void **real_head = (volatile void **)trie->head;
		int sum = 0;
		int i;
		for (i = 0; i < elements_num; i++)
		{
			if (real_head[i] != NULL)
			{
				sum = sum + rec_trie_nodes_num(real_head[i], (trie->metadata.prefix_length / 2) + 1);
				//real_head[i] = NULL;
			}
		}
		sum = sum + 1;
		return sum;
	}

}

/* Destroys a given trie and deallocates its memory */
void trie_destroy(volatile trie_t *trie, ptst_t *ptst)
{


	/* If the trie is empty, return */
	if (trie->head == NULL)
		return;

	int trie_size = trie_nodes_num(trie);
	int nodes_counter = 0;

	printf("trie-destroy called\n");
	print_trie(trie);

	if (trie->metadata.prefix_length == 14)
	{
		/* The trie is with only 256 leaves */
		free_trie_leaves_node256((trie_val_t *)trie->head, ptst);
		nodes_counter = 1;
	}
	else if (trie->metadata.prefix_length == 15)
	{
		/* The trie is with only 16 leaves */
		free_trie_leaves_node16((trie_val_t *)trie->head, ptst);
		nodes_counter = 1;
	}
	else
	{
		volatile void **iterated_inner_nodes[7]; /* max depth is 8 but we don't need the leaves */
		int iterated_array_index[7]; /* max depth is 8 but we don't need the leaves */
		int elements_num[7];
		int i;
		int cur_level = trie->metadata.prefix_length / 2;
		int first_level = cur_level;

		for (i = cur_level; i < 7; i++)
		{
			iterated_array_index[i] = 0;
			elements_num[i] = 256;
		}
		if (trie->metadata.prefix_length % 2 != 0)
			elements_num[cur_level] = 16;

		/* Remove the trie in a DFS way */
		iterated_inner_nodes[cur_level] = (volatile void **)trie->head;
		while (iterated_array_index[cur_level] < elements_num[cur_level])
		{
			if (iterated_inner_nodes[cur_level][iterated_array_index[cur_level]] != NULL)
			{
				if (cur_level < 6)
				{
					iterated_inner_nodes[cur_level + 1] = (volatile void **)iterated_inner_nodes[cur_level][iterated_array_index[cur_level]];
					/* enter next level */
					cur_level++;
					iterated_array_index[cur_level] = -1;
				}
				else
				{
					/* The node is a leaf, release it */
					free_trie_leaves_node256((volatile trie_val_t *)iterated_inner_nodes[cur_level][iterated_array_index[cur_level]], ptst);
					nodes_counter++;
				}

			}
			
			if (iterated_array_index[cur_level] == elements_num[cur_level] - 1)
			{
				if (cur_level == first_level)
				{
					/* Freed the whole trie, we are left with the last element */
					if (elements_num[cur_level] == 256)
					{
						free_trie_inner_node256((volatile void **)iterated_inner_nodes[cur_level][iterated_array_index[cur_level]], ptst);
						nodes_counter++;
					}
					else
					{
						free_trie_inner_node16((volatile void **)iterated_inner_nodes[cur_level][iterated_array_index[cur_level]], ptst);
						nodes_counter++;

					}
					break;
				}

				/* return to the previous level and free the node we pointed to */
				iterated_array_index[cur_level] = 0;
				cur_level--;
				free_trie_inner_node256((volatile void **)iterated_inner_nodes[cur_level][iterated_array_index[cur_level]], ptst);
				nodes_counter++;
			}
			else
				iterated_array_index[cur_level]++;
		}


	}
	printf("Trie size is %d and we removed %d nodes\n", trie_size, nodes_counter);
	if (nodes_counter != trie_size)
	{
		printf("Trie size is %d and we removed %d nodes... - bug\n", trie_size, nodes_counter);
		print_trie(trie);
		exit(56);
	}
}

#endif	/* TO_TEST */



#ifdef OLD
void trie_destroy(trie_t *trie)
{
	ptst_t *ptst;

	ptst = critical_enter();

	/* If the trie is only leaves, there is nothing to remove (the head will be removed on leap-list node deallocation */
	if (trie->metadata.prefix_length < TRIE_KEY_MAX_DIGITS - 2)
	{
		trie_head_t *iterated_nodes[7]; /* max depth is 8 but we don't need the leaves */
		int iterated_array_index[7]; /* max depth is 8 but we don't need the leaves */
		int elements_num[7];
		int i;
		int cur_level = trie->metadata.prefix_length / 2;

		for (i = 0; i < 7; i++)
		{
			iterated_array_index[i] = 0;
			elements_num[i] = TRIE_INNER_ARRAY_SIZE;
		}
		elements_num[cur_level] = ((trie->metadata.prefix_length % 2 == 0) ? 256 : 16);

		/* Remove the trie in a DFS way */
		iterated_nodes[cur_level] = &(trie->head);
		while (iterated_array_index[cur_level] < elements_num[cur_level])
		{
			if (iterated_nodes[cur_level]->nodes[iterated_array_index[cur_level]] != NULL)
			{
				if (cur_level < 6)
				{
					iterated_nodes[cur_level + 1] = iterated_nodes[cur_level]->nodes[iterated_array_index[cur_level]];
					/* enter next level */
					cur_level++;
					iterated_array_index[cur_level] = -1;
				}
				else
				{
					/* The node is a leaf, release it */
					free_trie_node(iterated_nodes[cur_level]->nodes[iterated_array_index[cur_level]]);
					iterated_nodes[cur_level]->nodes[iterated_array_index[cur_level]] = NULL;
				}

			}
			
			if (iterated_array_index[cur_level] == elements_num[cur_level] - 1)
			{
				if (cur_level == (trie->metadata.prefix_length / 2))
					break;
				/* return to the previous level and free the node we pointed to */
				iterated_array_index[cur_level] = 0;
				cur_level--;
				free_trie_node(iterated_nodes[cur_level]->nodes[iterated_array_index[cur_level]]);
				iterated_nodes[cur_level]->nodes[iterated_array_index[cur_level]] = NULL;
			}
			else
				iterated_array_index[cur_level]++;
		}




	}
	trie->metadata.prefix_length = 0;
	trie->metadata.prefix = 0;

	critical_exit(ptst);
}
#endif /* OLD */

#ifdef	TRIE_DEBUG
#include <stdio.h>

 #define TRIE_QUEUE_SIZE 50000

 #define ENQUEUE(elem) {						\
		nodes_queue[nodes_queue_head] = elem;	\
		nodes_queue_head = (nodes_queue_head + 1) % TRIE_QUEUE_SIZE;	\
	}
 #define DEQUEUE(res) {							\
		res = nodes_queue[nodes_queue_tail];	\
		nodes_queue_tail = (nodes_queue_tail + 1) % TRIE_QUEUE_SIZE;	\
	}

void print_trie(volatile trie_t *trie)
{
	int cur_level = 0;
	int cur_node_num = 0;
	int nodes_counter = 0;
	unsigned int cur_prefix_length;
	volatile void *nodes_queue[TRIE_QUEUE_SIZE];
	int nodes_queue_head = 0;
	int nodes_queue_tail = 0;
	int next_level_start_index = 0;
	unsigned long prefix;
	int i;

	printf("** Printing trie\n");
	prefix = trie->metadata.prefix;
	printf("prefix_length = %d; prefix = %lx\n", trie->metadata.prefix_length, prefix);
	cur_prefix_length = trie->metadata.prefix_length;
	if (trie->head == NULL)
	{
		printf("Empty trie\n");
		return;
	}

	ENQUEUE(trie->head);
	next_level_start_index = nodes_queue_head;
	nodes_counter++;

	while (cur_prefix_length < 14)
	{
		printf("Nodes level %d:\n\t", cur_level);
		while (nodes_queue_tail != next_level_start_index)
		{
			volatile void *pop_val;
			DEQUEUE(pop_val);
			volatile void **cur_node = (volatile void **)pop_val;
			if (cur_prefix_length % 2 == 1)
			{
				printf("{n=%d ", cur_node_num);
				for (i = 0; i < 16; i++)
					if (cur_node[i] != NULL)
					{
						printf("%x[n%d] ", i, nodes_counter++);
						ENQUEUE(cur_node[i]);
					}
				printf("}");
			}
			else
			{
				printf("{n=%d ", cur_node_num);
				for (i = 0; i < 256; i++)
					if (cur_node[i] != NULL)
					{
						printf("%2x[n%d] ", i, nodes_counter++);
						ENQUEUE(cur_node[i]);
					}
				printf("}");
			}

			cur_node_num++;
		}

		printf("\n");
		next_level_start_index = nodes_queue_head;
		cur_prefix_length++;
		if (cur_prefix_length % 2 == 1)
			cur_prefix_length++;
		cur_level++;
	}

	printf("Leaves level %d:\n\t", cur_level);
	while (nodes_queue_tail != next_level_start_index)
	{
		volatile void *pop_val;
		DEQUEUE(pop_val);
		volatile trie_val_t *cur_node = (volatile trie_val_t *)pop_val;
		if (cur_prefix_length == 15)
		{
			printf("{l=%d ", cur_node_num);
			for (i = 0; i < 16; i++)
			{
				if (cur_node[i] != 0)
					printf("%x[v%d] ", i, (cur_node[i] - 1));
				else
					printf("%x[v ] ", i);
			}
			printf("}");
		}
		else if (cur_prefix_length == 14)
		{
			printf("{l=%d ", cur_node_num);
			for (i = 0; i < 256; i++)
			{
				if (cur_node[i] != 0)
					printf("%2x[v%d] ", i, (cur_node[i] - 1));
				else
					printf("%2x[v ] ", i);
			}
			printf("}");
		}
		cur_node_num++;
	}
	printf("\n");
}


#endif	/* TRIE_DEBUG */




