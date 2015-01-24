/******************************************************************************
 * skip_stm.c
 * 
 * Skip lists, allowing concurrent update by use of the STM abstraction.
 * 
 * Copyright (c) 2003, K A Fraser
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#define __SET_IMPLEMENTATION__

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "portable_defns.h"
#include "gc.h"
#include "stm.h"
#include "set.h"

#ifndef	USE_TRIE
#define USE_TRIE
#endif	/* !USE_TRIE */

#ifdef	USE_TRIE
#include "trie.h"
#include <strings.h> /* bzero */
#endif /* USE_TRIE */
#include "leap_stm.h"

#define ASSERT_GC(X) {if(X==0) exit(999);}
#define make_marked_ptr(_p)   ((void *)((unsigned long)(_p) | 1))
#define mark_abo(_p) if(is_marked_ref(_p)) __transaction_cancel;
#define UNMARK(X) X=get_unmarked_ref(X)
#define MARK(X) X=make_marked_ptr(X)

static int gc_id;
typedef void set_t;

set_t *db[MAX_ROW];


typedef struct node_t {
    volatile unsigned long live;
    volatile setkey_t      low;             // Range lower end.
    volatile setkey_t      high;            // Range high end.
    volatile unsigned long count;           // Population.
    volatile unsigned long level;           // Pointers number.
    volatile item_t data[NODE_SIZE];        // The actual key-value.
#ifdef	USE_TRIE
    volatile trie_t trie;
#endif	/* USE_TRIE */
    volatile struct node_t volatile * next[MAX_LEVEL];  // Pointers.
} node_t;

// **************** TEST FUNCTIONS  **********************************

static void print_node2(volatile node_t *n, char *prefix)
{
    int i;

    printf("%sNode data:\n", prefix);
    if (is_marked_ref(n)) printf("\t%sNode pointer was marked ***\n");
    UNMARK(n);
    printf("\t%sNode address: %p\n", prefix, n);
    printf("\t%salive: %d\n", prefix, n->live);
    printf("\t%slow: %lx\n", prefix, n->low);
    printf("\t%shigh: %lx\n", prefix, n->high);
    printf("\t%scount: %d\n", prefix, n->count);
    printf("\t%slevel: %d\n", prefix, n->level);
#ifdef	USE_TRIE
    printf("\t%strie_address: %p\n", prefix, &n->trie);
    print_trie(&n->trie);
#endif	/* USE_TRIE */
    for (i = 0; i < n->level; i++)
        printf("\t\t%snext[%d] = %p\n", prefix, i, n->next[i]);
    printf("\t%skeys: ", prefix);
    for (i = 0; i < n->count; i++)
        printf("%lx, ", n->data[i].key);
    printf("\n");
    printf("%sNode printing done!\n", prefix);
}

static void print_set(set_t *l)
{
    node_t *cur = l;
    printf("Set nodes:\n");

    while (cur != NULL)
    {
        print_node2(cur, "\t"); 
        __transaction_atomic
        {
            cur = cur->next[0];
        }
    }
}

static void print_node(node_t *n)
{
    print_node2(n, "");
}

// **************************************************

#ifdef	USE_TRIE
static void init_node_trie(node_t * n)
{
    bzero(&n->trie, sizeof(trie_t));
}
#endif	/* USE_TRIE */

/*
 * Random level generator. Drop-off rate is 0.5 per level.
 * Returns value 1 <= level <= NUM_LEVELS.
 */
static int get_level(ptst_t *ptst)
{
    unsigned long r = rand_next(ptst);
    int l = 1;
    r = (r >> 4) & ((1 << (MAX_LEVEL-1)) - 1);
    while ( (r & 1) ) { l++; r >>= 1; }
    return l;
}


/*
 * Search for first non-deleted node, N, with key >= @k at each level in @l.
 * RETURN VALUES:
 *  Array @pa: @pa[i] is non-deleted predecessor of N at level i
 *  Array @na: @na[i] is N itself, which should be pointed at by @pa[i]
 *  MAIN RETURN VALUE: same as @na[0], direct pointer open for reading.
 */

static volatile node_t *search_predecessors(node_t *l, setkey_t k, volatile node_t **pa, volatile node_t **na)
{
    volatile node_t  *x,  *x_next;
    int i;
    ptst_t *ptst;
    unsigned long cnttt = 0;
restart_look:

    {
        x = l;
		// Go over all levels, top to bottom to find all predecessors and their successeors of the node that might contain key.
        for ( i = MAX_LEVEL - 1; i >= 0; i-- )
        {
            for ( ; ; )
            {
                x_next = x->next[i];
				// If pointer to next node is marked to be removed or next node isn't live anymore then restart search.
                if (is_marked_ref(x_next) || (!x_next->live))
                {
                    goto restart_look;
                }
				// Found upper bound, proceed to next level
                if (x_next->high >= k) 
                {
                    break;
                }
                else
                {
                    x=x_next;
                }
            }
			// If pointer to this/next node is marked then restart search.
            if (is_marked_ref(x) || is_marked_ref(x_next))
                goto restart_look;
            if ( pa ) pa[i] = x;
            if ( na ) na[i] = x_next;
        }

    }

    return x_next;
}

/* Deallocates a node and its trie) */
static void deallocate_node(node_t *n, ptst_t *ptst)
{
#ifdef	USE_TRIE
    volatile trie_t trie = n->trie;
#endif	/* USE_TRIE */
    gc_free(ptst, (node_t*)n, gc_id);
#ifdef	USE_TRIE
    trie_destroy(&trie, ptst);
#endif	/* USE_TRIE */
}

/*
 * PUBLIC FUNCTIONS
 */

/*
 * Allocate an empty set of lists. Returns a pointer to the database containing the lists.
 * The database that will contain MAX_ROW lists.
 */
set_t **set_alloc(void)
{
    ptst_t  *ptst;
    node_t *leap, *l;
    int i, j;

    ptst        = critical_enter();
    for(j=0; j<MAX_ROW; j++)
    {
        l           = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(l);

		// Initialize each list with 2 nodes , first is the sentinel that will contain no elements , second is a node that will be associated with all keys range. 
		// When the node reach its maximal size it will split into to two new nodes.

		// 'l' the second node, 'leap' the first sentinel one.
		
        l->low      = SENTINEL_KEYMIN;
        l->high     = SENTINEL_KEYMAX;
        l->count    = 0;
        l->live = 1;
        l->level = MAX_LEVEL;
        for(i = 0 ; i < MAX_LEVEL ; i++)
        {
            l->next[i] = NULL;
        }


#ifdef	USE_TRIE
        init_node_trie(l);
#endif	/* USE_TRIE */

        leap        = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(leap);
        leap->high  = SENTINEL_KEYMIN;
        leap->level = MAX_LEVEL;
        for(i = 0 ; i < MAX_LEVEL ; i++)
        {
            leap->next[i] = l;
        }
        leap->live = 1;

#ifdef	USE_TRIE
        init_node_trie(leap);
#endif	/* USE_TRIE */
        db[j] = (set_t *) leap;
    }
    critical_exit(ptst);

	//Returns a pointer to the set of lists.
    return db;
}

// Gets a node and a key, returns the value of the associated key if found in node 'n', 0 otherwise.
setval_t find(volatile node_t *n, setkey_t k)
{
    int i;

    if(n)
    {
#ifdef	USE_TRIE
		//When using Trie call the "trie_find_val" function
        if (n->count > 0)
        {
            trie_val_t res = trie_find_val(&n->trie, k);
            if (res != TRIE_KEY_NOT_FOUND)
            {
                return n->data[res].value;
            }

        }
#else	/* !USE_TRIE */
		// If Trie isn't used, go over all elements in the data array of n in order to find the given key.
        for(i=0; i<n->count; i++)
        {
            if(n->data[i].key == k)
                return n->data[i].value;
        }
#endif	/* !USE_TRIE */

    }
    return 0;
}

// called by set_remove.
// Gets an array of old_node (contains 2 in case there's a merge) , node n to set the new key-value pairs to, key 'k' to remove, 'merge' - 1 to merge/0 not to merge,
// ptst - per thread state managment, used for memory handling.

// Return 1 if given key was found and removed, 0 otherwise.
int remove(volatile node_t **old_node, node_t *n, setkey_t k, int merge, ptst_t *ptst)
{
    int i,j;
    int changed = 0;

	// copy all key-value pairs of old node to new one except the one associated with the given key 'k'
    for (i=0,j=0; j<old_node[0]->count; j++)
    {
        if(old_node[0]->data[j].key != k)
        {
            n->data[i].key = old_node[0]->data[j].key;
            n->data[i].value = old_node[0]->data[j].value;
            i++;
        }
        else
        {
            changed = 1;
            n->count--;
        }
    }

	// If node is merged, copy all keys from old_node[1] to node n as well.
    if(merge)
    {
        for (j=0; j<old_node[1]->count; j++)
        {
            n->data[i].key = old_node[1]->data[j].key;
            n->data[i].value = old_node[1]->data[j].value;
            i++;
        }
    }
#ifdef	USE_TRIE
	// If trie is used, build a trie to allow quick access to the key's index in the data array.
    trie_create_from_array(&n->trie, n->data, n->count, ptst);
#endif	/* USE_TRIE */
    return changed;
}

// called by set_update.

// Gets an array of new_node (contains 2 in case there's a split) to set the new values in , node n to take the old key-value pairs from, key 'k' & value 'v' to insert/update,
// 'split' - 1 to split/0 not to split,
// ptst - per thread state managment, used for memory handling.

// Return 1 if key-value pair was added/updated , 0 otherwise.
int insert(node_t **new_node,  volatile node_t *n, setkey_t k, setval_t v, int split, ptst_t *ptst)
{
    int i=0, j=0, changed = 0, m = 0;

    static int cnt = 0;

	// If there's a split, put NODE_SIZE/2 elements in one node and the rest in the other.
	// The 2 new nodes are now associated with the key range that was assoicated with one node 'n'

	// Otherwise, copy old 'n' properties to the new node
    if(split)
    {
        new_node[0]->low   = n->low;
        new_node[0]->count = NODE_SIZE/2;
        new_node[1]->high  = n->high;
        new_node[1]->count = n->count - (NODE_SIZE/2);
    }
    else
    {
        new_node[0]->low   = n->low;
        new_node[0]->high  = n->high;
        new_node[0]->count = n->count;
    }    

	// Add/update the given key-value pair (k,vk),also  add all elements from node 'n' to the new set of nodes (divide between 2 node if there's a split )
    if(n->count==0)
    {
        new_node[m]->data[0].key   = k;
        new_node[m]->data[0].value = v;
        new_node[m]->count         = 1;
        changed                    = 1;
#ifdef	USE_TRIE
        /* build a trie for a single value */
        trie_create_new(k, 0, &new_node[m]->trie, ptst);
#endif	/* USE_TRIE */
    }
    else
    {
       for(i=0, j=0; j<n->count; i++,j++)
        {
			// If key was found in old node update it's value and assign it to the new node.
			//Otherwise , add it like a normal key-value pair from node 'n'
            if(n->data[j].key == k)
            {
				new_node[m]->data[i].key = n->data[j].key;
                new_node[m]->data[i].value = v;
                changed = 1;
            }
			else
			{
				// Data is a sorted array therefore if next element has a bigger key then the given key 'k', that's is the place to put the given key-value pair into.
	            if((!changed) && (n->data[j].key > k))
	            {
	                new_node[m]->data[i].key = k;
	                new_node[m]->data[i].value = v;
	                new_node[m]->count++;
	                changed = 1;

					// Move to next node if split == 1 and new_node has reached it's assigned capcity 					
	                if((!m) && split && (new_node[0]->count == (i+1)))
	                {
	                    new_node[m]->high =  new_node[m+1]->low = new_node[m]->data[i].key;
	                    i = -1;
	                    m = m + 1;
	                }

	                i++;
	            }

				//Copy elemetns from old node 'n' to the new node.
	            new_node[m]->data[i].key = n->data[j].key;
	            new_node[m]->data[i].value = n->data[j].value;
			}

			// Move to next node if split == 1 and new_node has reached it's assigned capcity 
            if((!m) && split && (new_node[0]->count == (i+1)))
            {
                new_node[m]->high =  new_node[m+1]->low = new_node[m]->data[i].key;
                i = -1;
                m = m + 1;
            }

        }

		// In case the given key-value pair should be inserted in the end of the array, insert it here.
        if(!changed)
        {
            new_node[m]->count++;
            new_node[m]->data[i].key = k;
            new_node[m]->data[i].value = v;
            changed=1;
        }

#ifdef	USE_TRIE
        if (split)
        {
            /* build the tries for the new nodes */
            trie_create_from_array(&new_node[0]->trie, new_node[0]->data, new_node[0]->count, ptst);
            trie_create_from_array(&new_node[1]->trie, new_node[1]->data, new_node[1]->count, ptst);
        }
        else 
        {
            /* build the trie for the new node */
            trie_create_from_array(&new_node[m]->trie, new_node[m]->data, new_node[m]->count, ptst);
        }
#endif	/* USE_TRIE */

    }
    return changed;
}

/*
 * Add/Update mapping (@k[i] -> @v[i]) into list @l[i], where  0<= i < size . 
 * @Size - number of elements in the given arrays 
 */
void set_update(set_t **l, setkey_t *k, setval_t *v, int size)
{
	// ptst - per thread state managment, used for memory handling.
    ptst_t   *ptst;
	// Preds, succs - two dimensional array, each pred[i]/succ[i] is filled by search_predeccessors
    volatile node_t *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *n[MAX_ROW];
    int j, i, indicator = 0, changed[MAX_ROW], split[MAX_ROW];
    unsigned long max_height[MAX_ROW];
    node_t *new_node[MAX_ROW][2];

    ptst = critical_enter();

	// Init and allocate memory for new nodes for all lists in l
    for(j = 0; j<size ; j++)
    {
        new_node[j][0] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(new_node[j][0]);
        new_node[j][1] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(new_node[j][1]);
        new_node[j][0]->live = 0;
        new_node[j][1]->live = 0;
    }
retry_update:
	// Go over all lists in l, decide on their levels and if split is required or not.
    for(j = 0; j<size; j++)
    {

	// Initialize a new node trie to be associated with the new nodes.
#ifdef	USE_TRIE
        init_node_trie(new_node[j][0]);
        init_node_trie(new_node[j][1]);
#endif	/* USE_TRIE */

		// Get the predeccsors (in all of the levels ) of the node that k[j] should be added to into preds[j], get the successors of preds[j] into succs[j].
		// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node (or 2 new nodes is split is required), this node is returned
		//	into n[j].
        n[j] = search_predecessors(l[j], k[j] + SENTINEL_DIFF, preds[j], succs[j]);
		// If the node to be removed has reached its maximum size, it should be splitted into 2 new nodes.Otherwise only one node will replace it.
        if(n[j]->count == NODE_SIZE)
        {
            split[j] = 1; 
			// When splitting, one node will have the same level as the old node, and the other will get a random level ( With drop-off rate of 0.5 per level.)
            new_node[j][1]->level = n[j]->level;
            new_node[j][0]->level = get_level(ptst);
            max_height[j] = (new_node[j][0]->level > new_node[j][1]->level) ? new_node[j][0]->level : new_node[j][1]->level;
        }
        else
        {
            split[j] = 0;
            new_node[j][0]->level = n[j]->level; 
            max_height[j] = new_node[j][0]->level;
        }

		// Copy values of the old node to the new node ( or 2 if splitted ).
        changed[j] = insert(new_node[j], n[j], k[j] + SENTINEL_DIFF, v[j], split[j], ptst);

    }

	// Start a software transaction. 
	// Only this set of commands is surrounded by a transaction scope since the other operations are done on the new node
	// which isn't a part of the data strucure yet , so no thread would access it anyway.
	// Later on, when linking the new node, it will still be marked as not live while linking, therefore other operations will retry accessing it until it's full linked and live.

	// If any part of the transcation fails or aborted, all the operations untill that point will be reverted 
	// At this point the code will go back to retry_update to try and update all the lists again.
    __transaction_atomic 
    { 
		// Go over all lists and make sure all predeccessors 'next' pointers still point to the excpected nodes. 
		// Meaning, point to n[j] until n[j]->level and point to succs[j][i] at higher levels.
		// If n[j] or any of the predeccessors or successors are not live any more, abort the transcation.
		// After the verification level is passed, mark all 'next' pointers of n[j] and 'next' pointer of the predeccessors.
		// If any of the pointers is alreay marked, abort the transcation .
		// Set n[j] as not live at the end of the transacation.
        for(j = 0; j<size; j++)
        {
            if (n[j]->live == 0)
                __transaction_cancel;

            for(i = 0; i < n[j]->level; i++)
            {   
                if(preds[j][i]->next[i] != n[j]) __transaction_cancel;
				// If the node that is pointed by n[j] isn't live any more, abort the transcation.
                if(n[j]->next[i]) if(!n[j]->next[i]->live) __transaction_cancel;
            }

            for(i = 0; i < max_height[j]; i++)
            {   
				// If the linkage is not linked as expected anymore or any of the preds or succs isn't live anymore, abort the transcation.
                if(preds[j][i]->next[i] != succs[j][i]) __transaction_cancel;
                if(!(preds[j][i]->live)) __transaction_cancel;
                if(!(succs[j][i]->live)) __transaction_cancel;
            }



            if(changed[j])
            {
                for(i = 0; i < n[j]->level; i++)
                {
                    if (n[j]->next[i] != NULL)
                    {
						// If already marked, abort transacation.
                        mark_abo(n[j]->next[i]);
                        MARK(n[j]->next[i]);
                    }
                }                        

                for(i = 0; i < max_height[j]; i++)
                {
					// If already marked, abort transacation.
                    mark_abo(preds[j][i]->next[i]);
                    MARK(preds[j][i]->next[i]);
                }

                n[j]->live = 0;
            }


        }

		//Mark transcation as successful.
        indicator = 1;

    }
    if(!indicator)
    {
		// If transcation failed, deallocte the allocated tries and restart.
        for(j = 0; j<size; j++)
        {
#ifdef	USE_TRIE
            /* deallocate the tries */
            trie_destroy(&new_node[j][0]->trie, ptst);
            if (split[j]) trie_destroy(&new_node[j][1]->trie, ptst);
#endif	/* USE_TRIE */
        }
        goto retry_update;
    }

	// For each list in l. Link the pointers of the new nodes to point to the successors in succs. Also, re-link the predeccessors pointers to point to the new nodes assigned.
	// After that the new nodes are accessible, they are part of the lists, therefore they are set to be live.
    for(j = 0; j<size; j++)
    {
        if(changed[j]) // unlock
        {
            // First point the next pointers of all level in the new node ( or 2 if there's a split)  to the assoicated successors in succs.
            if (split[j])
            {   

				// If there is a split, distinguish between two case: one where new_node[j][1]->level > new_node[j][0]->level and the opposite.
                if (new_node[j][1]->level > new_node[j][0]->level)
                {   
					//  Since new_node[j][1]->level = n[j]->level and new_node[j][1]->level > new_node[j][0]->level, point all 'next' pointers in new_node[j][0] to new_node[j][1],
					//  Next, copy all 'next' pointers from n[j] to new_node[j][1] (since they share the same maximum level)
                    for (i = 0; i < new_node[j][0]->level; i++)
                    {
                        new_node[j][0]->next[i] = new_node[j][1];
						// unmark pointer
                        new_node[j][1]->next[i] = get_unmarked_ref(n[j]->next[i]);
                    }
                    for (; i < new_node[j][1]->level; i++)
						// unmark pointer
                        new_node[j][1]->next[i] = get_unmarked_ref(n[j]->next[i]);
                }
                else
                {   
					// In this case only point part of new_node[j][0] 'next' pointers to new_node[j][1]. The others should point to succs[j][i] , where i represents the higher levels.
					// This is because new_node[j][0] max level is bigger the original's node n[j]'s max level.
                    for (i = 0; i < new_node[j][1]->level; i++)
                    {
                        new_node[j][0]->next[i] = new_node[j][1];
						// unmark pointer
                        new_node[j][1]->next[i] = get_unmarked_ref(n[j]->next[i]);
                    }
                    for (; i < new_node[j][0]->level; i++)
                        new_node[j][0]->next[i] = succs[j][i];
                }
            }
            else
            {
				// If no split occuredm, simply copy all 'next' pointers from n[j] to new_node[j][0].
                for (i = 0; i < new_node[j][0]->level; i++)
                {
					// unmark pointer
                    new_node[j][0]->next[i] = get_unmarked_ref(n[j]->next[i]);
                }
            }

            // Link the predecessors 'next' pointers of the node n[j] to the new node ( or 2 if there's a split )
            for(i=0; i < new_node[j][0]->level; i++)
            {
                preds[j][i]->next[i] = new_node[j][0];
            }

			// If there's a split and new_node[j][1]->level > new_node[j][0]->level , Link the predecessors 'next' pointers of the node n[j] to new node[j][1]
            if (split[j] && (new_node[j][1]->level > new_node[j][0]->level))
                for(; i < new_node[j][1]->level; i++)
                {
                    preds[j][i]->next[i] = new_node[j][1];
                }

			// Linking completed, mark the new nodes as live.
            new_node[j][0]->live = 1;
            if (split[j])
                new_node[j][1]->live = 1;
        }


		// Deallocate unused nodes 
        if(changed[j])
        {
            deallocate_node(n[j], ptst);
            if (!split[j]) deallocate_node(new_node[j][1], ptst);
        }
        else
        {
            deallocate_node(new_node[j][0], ptst);
            deallocate_node(new_node[j][1], ptst);
        }    

    }
    critical_exit(ptst);
}


/*
 * Remove mapping for key @k[i] from set @l[i], where  0<= i < size.
 * @Size - number of elements in the given arrays 
 */
void set_remove(set_t **l, setkey_t *k, int size)
{
    ptst_t *ptst;
	// Preds, succs - two dimensional array, each pred[i]/succ[i] is filled by search_predeccessors
    volatile node_t *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *old_node[MAX_ROW][2];
    int i, j, total[MAX_ROW], indicator = 0, changed[MAX_ROW], merge[MAX_ROW];
    int indicator2 = 0;
    node_t *n[MAX_ROW];

    ptst = critical_enter();
	// Init and allocate memory for new nodes for all lists in l
    for(j=0; j<size; j++)
    {
        n[j] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(n[j]);
    }
retry_remove:
    for(j=0; j<size; j++)
    {
#ifdef	USE_TRIE
        init_node_trie(n[j]);
#endif	/* USE_TRIE */
    }

	// Go over all lists in l, if the associated key isn't present no remove is required so set changed[j] to 0 and continue to the next list.
	// Also, decide on their levels and if merge is required or not.
    for(j=0; j<size; j++)
    {
retry_last_remove:
        merge[j] = 0;

		// Get the predeccsors (in all of the levels ) of the node that k[j] should be added to into preds[j], get the successors of preds[j] into succs[j].
		// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node, this node is returned
		//	into old_node[j].
        old_node[j][0] = search_predecessors(l[j], k[j] + SENTINEL_DIFF, preds[j], succs[j]);

        /* If the key is not present, just return */
        if (find(old_node[j][0], k[j] + SENTINEL_DIFF) == 0)
        {
            changed[j] = 0;
            continue;
        }

		// Get the next node of old_node[j][0] in order to decide later on if merge is needed or not.
        do
        {
            old_node[j][1] = old_node[j][0]->next[0];
			// if old_node[j][0] isn't live anymore, find the old_node that contains the key again - goto retry_last_remove.
            if (!old_node[j][0]->live)
                goto retry_last_remove;
        } while (is_marked_ref(old_node[j][1]));

		// if old_node[j][0] isn't live anymore, find the old_node that contains the key again - goto retry_last_remove.
        if (!old_node[j][0]->live)
            goto retry_last_remove;

        total[j] = old_node[j][0]->count;

        if(old_node[j][1])
        {
            total[j] = total[j] + old_node[j][1]->count;

		// if any of the nodes old_node[j][0] or old_node[j][1] isn't live anymore, find the old_node that contains the k again - goto retry_last_remove.
            if (!old_node[j][0]->live || !old_node[j][1]->live)
                goto retry_last_remove;

			// if(total[j] <= NODE_SIZE) a merge is required, so the two nodes will become one new node.
            if(total[j] <= NODE_SIZE)
            {
                merge[j] = 1; 
            }
        }

		// Copy the old_node's properties to the new one.
        n[j]->level = old_node[j][0]->level;    
        n[j]->low   = old_node[j][0]->low;
        n[j]->count = old_node[j][0]->count;
        n[j]->live = 0;

		// If a merge is required, update the new node's properties. Get the maximum level between both of the old nodes and increase the number of elements and maximum expected key in the new node.
        if(merge[j])
        {
            if (old_node[j][1]->level > n[j]->level)
            {
                n[j]->level = old_node[j][1]->level;
            }
            n[j]->count += old_node[j][1]->count;
            n[j]->high = old_node[j][1]->high;
        }
        else
        {
            n[j]->high = old_node[j][0]->high;
        }

		// If the old nodes ( might be old_node[j][1] in case there's a merge ) isn't live any more retry the remove.
        if (!old_node[j][0]->live)
            goto retry_last_remove;

        if (merge[j] && !old_node[j][1]->live)
            goto retry_last_remove;

		// Call remove in order to copy all key-value pairs from the old node ( or 2 nodes if merge is required ) without the key to be removed.
        changed[j] = remove(old_node[j], n[j], k[j] + SENTINEL_DIFF, merge[j], ptst);

    }




	// Start a software transaction. 
	// Only this set of commands is surrounded by a transaction scope since the other operations are done on the new node
	// which isn't a part of the data strucure yet , so no thread would access it anyway.
	// Later on, when linking the new node, it will still be marked as not live while linking, therefore other operations will retry accessing it until it's full linked and live.

	// If any part of the transcation fails or aborted, all the operations untill that point will be reverted 
	// At this point the code will go back to retry_remove to try and remove keys from all the lists again.
    __transaction_atomic 
    {
		// Go over all lists and make sure all predeccessors 'next' pointers still point to the excpected nodes. 
		// If old_node[j][0] or old_node[j][1] ( if there's a merge ) or any of the predeccessors or successors are not live any more, abort the transcation.
		// After the verification level is passed, mark all 'next' pointers of old_node[j][0] ( and old_node[j][1] if there's a merge) and 'next' pointer of the predeccessors.
		// If any of the pointers is alreay marked, abort the transcation .
		// Set old_node[j][0] ( and old_node[j][1] if there's a merge ) as not live at the end of the transacation.

		
        for(j=0; j<size; j++)
        {
            if(changed[j])
            {
                if (!old_node[j][0]->live)
                    __transaction_cancel;

                if (merge[j] && !old_node[j][1]->live)
                    __transaction_cancel;

                for(i = 0; i < old_node[j][0]->level;i++)
                {
                    if (preds[j][i]->next[i] != old_node[j][0])  __transaction_cancel;
                    if (!(preds[j][i]->live))  __transaction_cancel;
                    if (old_node[j][0]->next[i]) if (!old_node[j][0]->next[i]->live) __transaction_cancel;
                }


                if (merge[j])
                {   
                    // Already checked that old_node[0]->next[0] is live, need to check if they are still connected
                    if (old_node[j][0]->next[0] != old_node[j][1])
                        __transaction_cancel;

                    if (old_node[j][1]->level > old_node[j][0]->level)
                    {   
                        // Up to old_node[0] height, we only need to validate the next nodes of old_node[1]
                        for (i = 0; i < old_node[j][0]->level; i++)
                        {
                            if (old_node[j][1]->next[i]) if (!old_node[j][1]->next[i]->live)  __transaction_cancel;
                        }
                        // For the higher part, we need to check also the preds of that part
                        for (; i < old_node[j][1]->level; i++)
                        {
                            if (preds[j][i]->next[i] != old_node[j][1])  __transaction_cancel;
                            if (!(preds[j][i]->live))  __transaction_cancel;
                            if (old_node[j][1]->next[i]) if (!old_node[j][1]->next[i]->live) __transaction_cancel;
                        }

                    }
                    else // old_node[0] is higher than old_node[1], just check the next pointers of old_node[1]
                    {
                        for (i = 0; i < old_node[j][1]->level; i++)
                        {
                            if (old_node[j][1]->next[i]) if (!old_node[j][1]->next[i]->live)  __transaction_cancel;
                        }
                    }
                }

                // Mark the pointers to the next nodes
                if(merge[j])
                {
                    for(i = 0; i < old_node[j][1]->level; i++)
                    {
                        if (old_node[j][1]->next[i] != NULL)
                        {   
                            mark_abo(old_node[j][1]->next[i]);
							// unmark pointer
                            MARK(old_node[j][1]->next[i]);
                        }
                    }
                    for(i = 0; i < old_node[j][0]->level; i++)
                    {
                        if (old_node[j][0]->next[i] != NULL)
                        {   
                            mark_abo(old_node[j][0]->next[i]);
							// unmark pointer
                            MARK(old_node[j][0]->next[i]);
                        }
                    }
                }
                else
                {   
                    for(i = 0; i < old_node[j][0]->level; i++)
                    {
                        if (old_node[j][0]->next[i] != NULL)
                        {   
                            mark_abo(old_node[j][0]->next[i]);
							// unmark pointer
                            MARK(old_node[j][0]->next[i]);
                        }
                    }
                }

                // Mark the pointers to the current node
                for(i = 0; i < n[j]->level; i++)
                {
                    mark_abo(preds[j][i]->next[i]);
					// unmark pointer
                    MARK(preds[j][i]->next[i]);
                }

                old_node[j][0]->live = 0;      
                if (merge[j])
                    old_node[j][1]->live = 0;      
            }
        }

		//Mark transcation as successful.
        indicator = 1;
    }

    if(!indicator)
    {
		// If transcation failed, deallocte the allocated tries and restart.
        for(j=0; j<size; j++)
        {
#ifdef	USE_TRIE
            trie_destroy(&n[j]->trie, ptst);
#endif	/* USE_TRIE */
        }
        goto retry_remove;
    }


	// For each list in l. Link the pointers of the new nodes to point to the successors in succs. Also, re-link the predeccessors pointers to point to the new nodes assigned.
	// After that the new nodes are accessible, they are part of the lists, therefore they are set to be live.
    for(j=0; j<size; j++)
    {
		// update links only if the removal was actually needed
        if(changed[j])
        {

            // Update the next pointers of the new node
            i = 0;

			// If a merge is required, get the 'next' pointers from old_node[j][1] into the new node 'n'. Later on if old_node[j][0] has a higher max level than old_node[j][1], node 'n' will get the 'next' pointers from it.
			// If no merge is required, copy all next pointers from old_node[j][0] into the new node 'n'.  
            if (merge[j])
            {   
                for (; i < old_node[j][1]->level; i++)
					// unmark pointer
                    n[j]->next[i] = get_unmarked_ref(old_node[j][1]->next[i]);
            }
            for (; i < old_node[j][0]->level; i++)
				// unmark pointer
                n[j]->next[i] = get_unmarked_ref(old_node[j][0]->next[i]);

			// n[j]-> level is the max level between old_node[j][0] and old_node[j][1] ( in case of a merge ), so the merge case is covered here as well.
            // Link the predecessors 'next' pointers of old_node[0] ( in case there's a merge and old_node[1]->level > old_node[0]->level old_node[1]'s preds will point to n[j] as well )
            //														to the new node n[j]. 
            for(i = 0; i < n[j]->level; i++)
            {   
                preds[j][i]->next[i] = n[j];
            }

			// Node is fully linked to the list, set it as live.
            n[j]->live = 1;


			// Deallocate unused nodes.
            if(merge[j])
                deallocate_node(old_node[j][1], ptst);

            deallocate_node(old_node[j][0], ptst);
        }

        else
        {
			// No change is needed - deallocate new assigned node.
            deallocate_node(n[j], ptst);
        }    
    }

    critical_exit(ptst);
}


/*
 * Look up mapping for key @k in list @l. Return value if found, else NULL.
 */
setval_t set_lookup(set_t *l, setkey_t k)
{
    volatile node_t *n;
    int i, indicator = 0;
    ptst_t *ptst;
    setval_t v;

    k=k+ SENTINEL_DIFF; // Avoid sentinel

    ptst = critical_enter();

	// Used here just to locate the node that k is supposed to be in.
    n = search_predecessors(l, k, 0, 0);

	// Call find to find the key k in node n. v is set to null if not found.
    v = find(n,k);

    critical_exit(ptst);

    return v;
}

/*
 * Traverse the list from low to high.
 */
setval_t set_rq(set_t *l, setkey_t low, setkey_t high)
{
    volatile node_t *n;
    int i, indicator = 0;
    ptst_t *ptst;

    low = low+ SENTINEL_DIFF; // Avoid sentinel
    high = high+ SENTINEL_DIFF;

    ptst = critical_enter();

retry_rq:
	// Used here just to locate the node that low is supposed to be in.
    n = search_predecessors(l, low, 0, 0);

	// Start a software transcation. The goal is to traverse on all nodes without any changes in the way in order to capture a correct snapshot of these set of nodes
	// Therefore, if one of the nodes in the way is discovered as not live anymore, abort the transcation and restart the entire search.
    __transaction_atomic 
    {
		// Traverse the list from the node that could contain low to the last node that could contain high.
        while(high>n->high)
        {
            if (!n->live)
                __transaction_cancel;
            n = get_unmarked_ref(n->next[0]);
        }

		// Mark transcation as successful .
        indicator = 1;
    }
	// On failure, restart search and traverse again.
    if(!indicator)
        goto retry_rq;

    critical_exit(ptst);

    return 0;


}

// Get a GC allocator ID from the GC and init the trie sub system.
void _init_set_subsystem(void)
{
    gc_id =     gc_add_allocator(sizeof(node_t));
#ifdef	USE_TRIE
    _init_trie_subsystem();
#endif	/* USE_TRIE */
}

