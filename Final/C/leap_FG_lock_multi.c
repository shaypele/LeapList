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

#define EINVAL 22

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
    volatile unsigned long 		isMarked;		// Is marked to be removed
    pthread_mutex_t 	lock ;			// lock 

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
                if (!x_next->live)
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
			
            if ( pa ) pa[i] = x;
            if ( na ) na[i] = x_next;
        }

    }
	
    return x_next;
}

/* Deallocates a node and its trie) */
static void deallocate_node(node_t *n, ptst_t *ptst)
{
	pthread_mutex_destroy(&n->lock);
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
		l->isMarked = 0;
		if (pthread_mutex_init(&l->lock, NULL) != 0)
	    {
	        printf("\n mutex init failed\n");
	        exit(9);
	    }
		
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
		leap->isMarked = 0;

		if (pthread_mutex_init(&leap->lock, NULL) != 0)
	    {
	        printf("\n mutex init failed\n");
	        exit(9);
	    }

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

// Used by set update. Get an array of predeccessors and the highest locked level.
// Unlocks all locks of the nodes of these predeccessors  untill highestLocked node is reached (inclusive)
void unlockPredForUpdate(volatile  node_t **pa,int highestLocked)
{
	node_t *prevPred = 0x0;
	int j;
	for ( j = 0 ; j <= highestLocked ; j++ ){
		if ( pa[j]!=prevPred )
		{
			pthread_mutex_unlock(&pa[j]->lock);
			prevPred = pa[j];
		}
	}
}

/*
 * Add/Update mapping (@k[i] -> @v[i]) into list @l[i], where  0<= i < size . 
 * @Size - number of elements in the given arrays 
 *
 * Fine grained locking implemanation - Based on lazy skip list. 
 * Lock the node that the key is in its range, then lock all its predecessors and then do the update.
 *
 */
void set_update(set_t **l, setkey_t *k, setval_t *v, int size)
{
	// ptst - per thread state managment, used for memory handling.
    ptst_t   *ptst;
	// Preds, succs - two dimensional array, each pred[i]/succ[i] is filled by search_predeccessors
    volatile node_t *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *n[MAX_ROW];
    int j, i, changed[MAX_ROW], split[MAX_ROW];
    unsigned long max_height[MAX_ROW];
    node_t *new_node[MAX_ROW][2];
    int level;
	node_t* lastLockedNode = 0x0;
	// isMarked - 1 if node is marked by this thread, 0 otherwise. highestLocked - highest level locked.
	int isMarked, highestLocked = -1, valid = 1;
	node_t *pred = 0x0,*succ = 0x0,*prevPred = 0x0;

    ptst = critical_enter();

	// Init and allocate memory for new nodes for all lists in l
    for(j = 0; j<size ; j++)
    {
        new_node[j][0] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(new_node[j][0]);
        new_node[j][1] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(new_node[j][1]);
        new_node[j][0]->live = 0;
		new_node[j][0]->isMarked = 0;
        new_node[j][1]->live = 0;
		new_node[j][1]->isMarked = 0;
    }
	
	for(j = 0; j<size; j++)
    {
		lastLockedNode  = 0x0;
		isMarked = 0;
	retry_update:
	// Decide on the new node's max level and decide whether a split is required or not.
				
		highestLocked = -1;
		valid = 1;
		pred = 0x0;
		succ = 0x0;
		prevPred = 0x0;

    
if (pthread_mutex_init(&new_node[j][0]->lock, NULL) != 0)
{
	printf("\n mutex init failed\n");
	exit(9);
}

if (pthread_mutex_init(&new_node[j][1]->lock, NULL) != 0)
{
	printf("\n mutex init failed\n");
	exit(9);
}
	// Initialize a new node trie to be associated with the new nodes.
#ifdef	USE_TRIE
        init_node_trie(new_node[j][0]);
		
        init_node_trie(new_node[j][1]);
		
#endif	/* USE_TRIE */

		// Get the predeccsors (in all of the levels ) of the node that k[j] should be added to into preds[j], get the successors of preds[j] into succs[j].
		// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node (or 2 new nodes is split is required), this node is returned
		//	into n[j].
        n[j] = search_predecessors(l[j], k[j] + SENTINEL_DIFF, preds[j], succs[j]);

		// In case there was a restart and the key now is under a different node, unlock & unmark previously locked node ( if needed ) 
		if (lastLockedNode != 0x0 && lastLockedNode != n[j]){
			if (lastLockedNode->isMarked){
				pthread_mutex_unlock(&lastLockedNode->lock);
				lastLockedNode->isMarked = 0;
				isMarked = 0;
			}
		}

		// Continue with the update if the node that contains the key is marked by this thread or if the node is not marked and is live.
		// Otherwise retry opertation.
		if ( isMarked || (!n[j]->isMarked && n[j]->live )  )
		{

			// If node is not marked yet by this thread, it needs to be locked and marked.
			if ( !isMarked )
			{
				int err = 0,isLockFailed = 0;
				// Try to lock the node. If the locking fails because of the node not being initialized it means that the node is no longer alive.
				// Therefore the operation shouold be restarted, so the new node that the key is in its range would be found.
				while ((err = pthread_mutex_trylock(&n[j]->lock)) && !isLockFailed){
					if (err == EINVAL){ // not initialized , so probably not live
						isLockFailed = 1;
					}
				}

				// restart operation if the node is no longer live or it has been marked to be removed.
				if (n[j]->isMarked || !n[j]->live){

					// No reasond to unlock the lock if lock failed.
					if (!isLockFailed){
						 //Try to lock and is marked, then release lock and try again.
						pthread_mutex_unlock(&n[j]->lock);
					}

					 pthread_mutex_destroy(&new_node[j][0]->lock);
					 pthread_mutex_destroy(&new_node[j][1]->lock); 
				
					#ifdef	USE_TRIE
			            // deallocate the tries  
			            trie_destroy(&new_node[j][0]->trie, ptst);
						
			            if (split[j]) trie_destroy(&new_node[j][1]->trie, ptst);
						
					#endif	// USE_TRIE 
					goto retry_update;
				}

				// If locking was successful, marked node to be removed and set isMarked to true , 
				// 		so the thread would know it was the one who marked the node.
				lastLockedNode = n[j];
				n[j]->isMarked = 1;
				isMarked = 1;
			}

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

			// Lock all predeccessors of n[j] in order to remove the link and relink to the new node later.
			// Lock in an ascending order ( 0 to top level) in order to avoid deadlocks.
			// The following code would fail and restart the whold operation if one ( or more ) of these conditions occurs :
			//  a . Any of the locks fails 
			//  b. one of the predeccessors is marked to be removed
			//  c. one of the 'next' pointers of any of the predeccessors doesn't point to the expected node ( i.e. successor )
			// When any of these conditions occurs, a restart of the operation is in order. Before restarting unlock any predeccessors that were already locked. 
			// Also,destroy the newly created tries and the locks of the new nodes.
			
			for ( level = 0; valid && ( level < max_height[j] ); level++){
				pred = preds[j][level];
				succ = succs[j][level];
				// No need to lock twice if the same node is a predeccessor in a different level.
				if (pred != prevPred){  
						int err2 = 0;
						while (err2 = pthread_mutex_trylock(&pred->lock)){
							// Lock is not initialized,meaning the node is not live -> the operation is not valid any more
							if (err2 == EINVAL ){ 
								valid = 0;
								break;
							}
						}
						if (!valid){
							break;
						} 
					highestLocked = level;
					prevPred = pred;
				}
				// Invalidate operation if the predeccessor is marked to be removed or doesn't point to the expected successor 
				valid = !pred->isMarked && pred->next[level] == succ ;
			}
			if (!valid){
				// unlock all predeccessor from bottom level to highestLocked  (inclusive)
				unlockPredForUpdate( preds[j], highestLocked);
				
				pthread_mutex_destroy(&new_node[j][0]->lock);
				pthread_mutex_destroy(&new_node[j][1]->lock); 
				#ifdef	USE_TRIE
		            // deallocate the tries  
		            trie_destroy(&new_node[j][0]->trie, ptst);
				
		            if (split[j]) trie_destroy(&new_node[j][1]->trie, ptst);
					
				#endif	// USE_TRIE  
				goto retry_update;
			}	

		// Mark n[j] as not live anymore.
	    n[j]->live = 0;
		
			// Link the pointers of the new nodes to point to the successors in succs. Also, re-link the predeccessors pointers to point to the new nodes assigned.
			// After that the new nodes are accessible, they are part of the lists, therefore they are set to be live.
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
	                        new_node[j][1]->next[i] = (n[j]->next[i]);
	                    }
	                    for (; i < new_node[j][1]->level; i++)
	                        new_node[j][1]->next[i] = (n[j]->next[i]);
	                }
	                else
	                {   
					// In this case only point part of new_node[j][0] 'next' pointers to new_node[j][1]. The others should point to succs[j][i] , where i represents the higher levels.
					// This is because new_node[j][0] max level is bigger the original's node n[j]'s max level.
	                    for (i = 0; i < new_node[j][1]->level; i++)
	                    {
	                        new_node[j][0]->next[i] = new_node[j][1];
	                        new_node[j][1]->next[i] = (n[j]->next[i]);
	                    }
	                    for (; i < new_node[j][0]->level; i++)
	                        new_node[j][0]->next[i] = succs[j][i];
	                }
	            }
	            else
	            {
				// If no split occurred, simply copy all 'next' pointers from n[j] to new_node[j][0].
	                for (i = 0; i < new_node[j][0]->level; i++)
	                {
	                    new_node[j][0]->next[i] = (n[j]->next[i]);
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
	            {
	                			new_node[j][1]->live = 1;

	            }
			
	        }

 			// Unlock n[j] and all of its predeccessors 
			pthread_mutex_unlock(&n[j]->lock); 
			unlockPredForUpdate( preds[j], highestLocked);
			
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
		else
		{ 
			// n[j] isn't relevant anymore, destroy the locks of the new nodes and the newly created tries.
			pthread_mutex_destroy(&new_node[j][0]->lock);
			pthread_mutex_destroy(&new_node[j][1]->lock);
			
			#ifdef	USE_TRIE
		            // deallocate the tries  
		            
		            trie_destroy(&new_node[j][0]->trie, ptst);
		            if (split[j]) trie_destroy(&new_node[j][1]->trie, ptst);
					
				#endif	// USE_TRIE 
			goto retry_update;
		} 
    }
	
    critical_exit(ptst);
}

// Used by set update. Gets an array of predeccessors of "old_node[0]" an array of predeccessors of "old_node[1]" ( if there's an ongoing merge)
// @highestLocked - highest level locked 
// @levelOldNode0 - Used in case there a merge and that highestLocked is bigger than the max level of "old_node[0]"
// Unlocks all locks of the nodes of these predeccessors  untill highestLocked node is reached (inclusive)
void unlockPredForRemove(volatile  node_t **pa,volatile  node_t **pa_Node1 ,int highestLocked, int levelOldNode0 )
{
	node_t *prevPred = 0x0;
	 int iterateTill = -1;
     int level;

		 // First iterate on the predeccessors of "old_node[0]". 
		 // Iterate till highestLocked if highestLocked < levelOldNode0. Otherwise, Iterate till levelOldNode0 - 1.
    	 if ( highestLocked < levelOldNode0 ){
    		 iterateTill = highestLocked; 
    	 }
    	 else{
    		 iterateTill = levelOldNode0 - 1;
    	 }
    	 // First unlock all pred of node 0->
		 for (level = 0 ; level <= iterateTill ; level++ ){
		 	// Don't unlock same predecessor twice.
			if ( pa[level]!=prevPred )
			{
				pthread_mutex_unlock(&pa[level]->lock);
				prevPred = pa[level];
			}
		 }
		 
		 // if needed , unlock rest of preds of node 1->
		if ( pa_Node1 != 0 &&  highestLocked >= levelOldNode0 ){
			iterateTill = highestLocked; 
			for (; level <= iterateTill ; level++ ){
				// Don't unlock same predecessor twice.
				if ( pa_Node1[level]!=prevPred )
				{
					pthread_mutex_unlock(&pa_Node1[level]->lock);
					prevPred = pa_Node1[level];
				}
			}
		}



	
}

/*
 * Remove mapping for key @k[i] from set @l[i], where  0<= i < size.
 * @Size - number of elements in the given arrays 
 */
void set_remove(set_t **l, setkey_t *k, int size)
{
    ptst_t *ptst;
	// Preds, succs - two dimensional array, each pred[i]/succ[i] is filled by search_predeccessors
	volatile node_t volatile *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *old_node[MAX_ROW][2];
	volatile node_t volatile *preds_node1[MAX_ROW][MAX_LEVEL], *succs_node1[MAX_ROW][MAX_LEVEL];
	volatile node_t *pred, *succ, *prevPred = 0x0;   
    int i, j,  changed[MAX_ROW], merge[MAX_ROW];
    int valid = 1;
    node_t *n[MAX_ROW];
	node_t* lastLockedNode[2];

    ptst = critical_enter();

    for(j=0; j<size; j++)
    {
		// Init and allocate memory for new nodes 
		int highestLocked = -1;
		char isMarkedArr[2]; 
        isMarkedArr[0] = 0;
        isMarkedArr[1] = 0;
        n[j] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(n[j]);
		
		lastLockedNode[0] = 0x0;
		lastLockedNode[1] = 0x0;
    
retry_remove:
    	highestLocked = -1;
		prevPred = 0x0;
		valid = 1;
		
		if (pthread_mutex_init(&n[j]->lock, NULL) != 0)
	    {
	        printf("\n mutex init failed\n");
	        exit(9);
	    }
		
#ifdef	USE_TRIE
        init_node_trie(n[j]);

#endif	/* USE_TRIE */
    

	// If the associated key isn't present no remove is required so set changed[j] to 0 and continue to the next list.
	// Also, decide on their levels and if merge is required or not.
    
retry_last_remove:
        merge[j] = 0;
		// Get the predeccsors (in all of the levels ) of the node that k[j] should be added to into preds[j], get the successors of preds[j] into succs[j].
		// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node, this node is returned
		//	into old_node[j].
		old_node[j][0] = search_predecessors(l[j], k[j] + SENTINEL_DIFF, preds[j], succs[j]);
        /* If the key is not present ,continue to next key ( with the exception of the old node not being live, then retry the operation) */
        if (find(old_node[j][0], k[j] + SENTINEL_DIFF) == 0)
        {
			if (!old_node[j][0]->live){
        		 goto retry_last_remove;
        	 }
            changed[j] = 0;
			
			#ifdef	USE_TRIE
				trie_destroy(&n[j]->trie, ptst);
			#endif	/* !USE_TRIE */
			
			// Get the next node of old_node[j][0] 
			old_node[j][1] = old_node[j][0]->next[0];

			// In case this is a restart of the operation and old_node[0] was already marked by this thread, but the key wasn't found anymore,
			// then unlock the node's lock and unmark it.			
			if (old_node[j][0]->isMarked && isMarkedArr[0]){
					pthread_mutex_unlock(&old_node[j][0]->lock);
				old_node[j][0]->isMarked = 0;
				isMarkedArr[0] = 0;
			}

			// In case this is a restart of the operation and old_node[1] was already marked by this thread, but the key wasn't found anymore,
			// then unlock the node's lock and unmark it.			
			if (old_node[j][1]!=0 && old_node[j][1]->isMarked && isMarkedArr[1]){
				pthread_mutex_unlock(&old_node[j][1]->lock);
				old_node[j][1]->isMarked = 0;
				isMarkedArr[1] = 0;
			}
			
			deallocate_node(n[j], ptst);
			// continue to next key
            continue;
        }

		// In case there was a restart and the key now is under a different node, unlock & unmark previously locked node ( if needed ) 
		if (lastLockedNode[0] != 0x0 && lastLockedNode[0] != old_node[j][0]){
			if (lastLockedNode[0]->isMarked){
				pthread_mutex_unlock(&lastLockedNode[0]->lock);
			lastLockedNode[0]->isMarked = 0;
			isMarkedArr[0] = 0;
			};
		}

		// Get the next node of old_node[j][0] in order to decide if merge is needed or not.
		old_node[j][1] = old_node[j][0]->next[0];
		if (old_node[j][1]!= 0 && 
			// Merge threshold was reduced here to NODE_SIZE - 20. When threshold was set to NODE_SIZE, there were cases were  a split and a merge could happen concurrently. 
			// This could get the application to get stuck, this way a saftey margin of size 20 is taken in order to prevent that end case.
         		(old_node[j][0]->count + old_node[j][1]->count - 1) <= NODE_SIZE - 20 ) 
		{
         		merge[j] = 1;
		}
		else
		{
			merge[j] = 0;
		}
		
		// Continue with the update if the node that contains the key is marked by this thread or if the node is not marked and is live.
		// Otherwise retry opertation.
    	 if ( 	(isMarkedArr[0] || 
    			(  !old_node[j][0]->isMarked &&  old_node[j][0]->live ))  ){
				// if node is marked and not by this thread try to search the node once more.
				if (merge[j] && !(isMarkedArr[1] || 
										(  !old_node[j][1]->isMarked &&  old_node[j][1]->live ))  )
				{
					goto fail_merge;
				}
				
			// If no merge is decided, lock and mark only old_node[j][0]	
    		 if (!merge[j]){
			 	// If node is not marked yet by this thread, it needs to be locked and marked.
				 if (!isMarkedArr[0]){

					int  err = 0,isLockFailed = 0;
					// Try to lock the node. If the locking fails because of the node not being initialized it means that the node is no longer alive.
					// Therefore the operation should be restarted, so the new node that the key is in its range would be found.
					while ((err = pthread_mutex_trylock(&old_node[j][0]->lock)) && !isLockFailed){
						if (err == EINVAL){ // not initialized , so probably not live
								isLockFailed = 1;
							}
					}
					
					// restart operation if the node is no longer live or it has been marked to be removed.
					 if (old_node[j][0]->isMarked || !old_node[j][0]->live ){
					 	
					 	// No reason to unlock the lock if lock failed.
						if (!isLockFailed)
						{
							//Try to lock and is marked, then release lock and try again.
							pthread_mutex_unlock(&old_node[j][0]->lock);
						}

						#ifdef	USE_TRIE
							// deallocate the trie
							trie_destroy(&n[j]->trie, ptst);
						#endif	/* !USE_TRIE */
						
						pthread_mutex_destroy(&n[j]->lock);
						 goto retry_remove;
					 }

					 // If locking was successful, marked node to be removed and set isMarkedArr[0] to true , 
					// 		so the thread would know it was the one who marked the node.
					 lastLockedNode[0] = old_node[j][0];
					 old_node[j][0]->isMarked = 1;
					 isMarkedArr[0] = 1;
				}
    		}
			//merge , lock backwards . First Lock old_node[1] and then old_node[0]
			// This is done in order to avoid deadlocks and maintain the always lock backwards policiy ( as done with node locking and its predeccessors)
			else{ 
				// In case there was a restart and node to be merged with is different now, unlock & unmark previously locked node ( if needed ) 
				if (lastLockedNode[1] != 0x0 && lastLockedNode[1] != old_node[j][1]){
					if (lastLockedNode[1]->isMarked){
						pthread_mutex_unlock(&lastLockedNode[1]->lock);
					lastLockedNode[1]->isMarked = 0;
					isMarkedArr[1] = 0;
					}
				}
				
				// Mark and lock second node. It is enough to checked if old_node[0] was marked by this thread or not.
				if (!isMarkedArr[0]){
					
					// If node is not marked yet by this thread, it needs to be locked and marked.
					if (!isMarkedArr[1]){												 
						int  err = 0,isLockFailed = 0;
						
						// Try to lock the node. If the locking fails because of the node not being initialized it means that the node is no longer alive.
						// Therefore the operation should be restarted, so the new node that the key is in its range would be found.
						while ((err = pthread_mutex_trylock(&old_node[j][1]->lock)) && !isLockFailed){
							if (err == EINVAL){
									isLockFailed = 1;
								}
						}
						
						// restart operation if the node is no longer live or it has been marked to be removed.
						 if (old_node[j][1]->isMarked || !old_node[j][1]->live ){
						 	
						 	// No reason to unlock the lock if lock failed.
							if (!isLockFailed)
							{
								//Try to lock and is marked, then release lock and try again.
								pthread_mutex_unlock(&old_node[j][1]->lock);
							}
							
							#ifdef	USE_TRIE
							  // deallocate the trie
								trie_destroy(&n[j]->trie, ptst);
							#endif	/* !USE_TRIE */
							
							pthread_mutex_destroy(&n[j]->lock);
							 goto retry_remove;
						 }

						 // If locking was successful, marked node to be removed and set isMarked to true , 
						// 		so the thread would know it was the one who marked the node.
						 lastLockedNode[1] = old_node[j][1];
						 old_node[j][1]->isMarked = 1;
						 isMarkedArr[1] = 1;	


					}


					// Now try and lock old_node[j][0]

					
					int err4 = 0,isLockFailed2 = 0;
					// Try to lock the node. If the locking fails because of the node not being initialized it means that the node is no longer alive.
					// Therefore the operation shouold be restarted, so the new node that the key is in its range would be found.
					while ((err4 = pthread_mutex_trylock(&old_node[j][0]->lock)) && !isLockFailed2){
						if (err4 == EINVAL){ // not initialized , so probably not live
								isLockFailed2 = 1;
							}
					}
				
    				 // restart operation if the node is no longer live or it has been marked to be removed.
					 if (old_node[j][0]->isMarked || !old_node[j][0]->live ){

						// No reason to unlock the lock if lock failed.
						if (!isLockFailed2)
						{
							//Try to lock and is marked, then release lock and try again.
							pthread_mutex_unlock(&old_node[j][0]->lock);
						}

						#ifdef	USE_TRIE
							 // deallocate the tries  
							trie_destroy(&n[j]->trie, ptst);
						#endif	/* !USE_TRIE */
						
						
						pthread_mutex_destroy(&n[j]->lock);
						 goto retry_remove;
					 }

					 // If locking was successful, marked node to be removed and set isMarked to true , 
					// 		so the thread would know it was the one who marked the node.
					 lastLockedNode[0] = old_node[j][0];
					 old_node[j][0]->isMarked = 1;
					 isMarkedArr[0] = 1; 
				}
         	}

			 /********* Remove Setup ***********/
			 
    		 // Copy the old_node's properties to the new one.
	        n[j]->level = old_node[j][0]->level;    
	        n[j]->low   = old_node[j][0]->low;
	        n[j]->count = old_node[j][0]->count;
	        n[j]->live = 0;
			n[j]->isMarked = 0;
			
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
			
			// Call remove in order to copy all key-value pairs from the old node ( or 2 nodes if merge is required ) without the key to be removed.
			changed[j] = remove(old_node[j], n[j], k[j] + SENTINEL_DIFF, merge[j], ptst);
			/****** End Remove Setup ***********/

			
         	//first, lock all prevs of node 0-> Later on, if needed, lock preds of node 1->
         	int level;

			// Lock all predeccessors of old_node[j][0] in order to remove the link and relink to the new node later.
			// Lock in an ascending order ( 0 to top level) in order to avoid deadlocks.
			// The following code would fail and restart the whold operation if one ( or more ) of these conditions occurs :
			//  a . Any of the locks fails 
			//  b. one of the predeccessors is marked to be removed
			//  c. one of the 'next' pointers of any of the predeccessors doesn't point to the expected node ( i.e. successor )
			// When any of these conditions occurs, a restart of the operation is in order. Before restarting unlock any predeccessors that were already locked. 
			// Also,destroy the newly created tries and the locks of the new nodes.

			// If merge is needed and ( old_node[j][0]->level < old_node[j][1]->level ) do the same for old_node[j][1]'s predeccessors
         	for ( level = 0; valid && ( level < old_node[j][0]->level ); level++){
				pred = preds[j][level];
				succ = succs[j][level];
				// No need to lock twice if the same node is a predeccessor in a different level.
				if (pred != prevPred){

					int err2 = 0;
					while (err2 = pthread_mutex_trylock(&pred->lock)){
						// Lock is not initialized,meaning the node is not live -> the operation is not valid any more
						if (err2 == EINVAL ){
							valid = 0;
							break;
						}
					}
					if (!valid){
					break;
					}
					
					highestLocked = level;
					prevPred = pred;
				}
				// Invalidate operation if the predeccessor is marked to be removed or doesn't point to the expected successor 
				valid = !pred->isMarked && pred->next[level] == succ;
			}
			if (!valid){
				// unlock all predeccessor from bottom level to highestLocked  (inclusive)
				//preds_node1 won't be used here.
				unlockPredForRemove(preds[j] ,0x0 ,highestLocked, old_node[j][0]->level );
				
				#ifdef	USE_TRIE
					trie_destroy(&n[j]->trie, ptst);
				#endif	/* !USE_TRIE */
				
				pthread_mutex_destroy(&n[j]->lock);
				goto retry_remove;
			}	
			
			// if node 1's level is bigger than node 0's level, lock preds of higher level of node 1-> 
         	if ( merge[j] && ( old_node[j][0]->level < old_node[j][1]->level ) ){
         		// Find preds of node 1.
				search_predecessors(l[j], old_node[j][1]->high, preds_node1[j], succs_node1[j]);
         		for(; valid && ( level < old_node[j][1]->level ); level++){
         			pred = preds_node1[j][level];
					succ = succs_node1[j][level];
					// No need to lock twice if the same node is a predeccessor in a different level.
					if (pred != prevPred){
						int err3 = 0;
						while (err3 = pthread_mutex_trylock(&pred->lock)){
							// Lock is not initialized,meaning the node is not live -> the operation is not valid any more
							if (err3 == EINVAL ){
								valid = 0;
								break;
							}
						}
						if (!valid){
						break;
						}
							
						highestLocked = level;
						prevPred = pred;
					}
					// Invalidate operation if the predeccessor is marked to be removed or doesn't point to the expected successor 
					valid = !pred->isMarked && pred->next[level] == succ;
				}
				if (!valid){
					// unlock all predeccessor from bottom level to highestLocked  (inclusive)
					// unlock for predeccessors of node 1 as well if needed.
					unlockPredForRemove(preds[j] ,preds_node1[j] ,highestLocked, old_node[j][0]->level );
					
					#ifdef	USE_TRIE
						trie_destroy(&n[j]->trie, ptst);
					#endif	/* !USE_TRIE */
					
					pthread_mutex_destroy(&n[j]->lock);
					goto retry_remove;
				}	
         	}
         	
         	/******** Release and Update **********/
		// Link the pointers of the new nodes to point to the successors in succs. Also, re-link the predeccessors pointers to point to the new nodes assigned.
		// After that the new nodes are accessible, they are part of the lists, therefore they are set to be live.		
		
		// update links only if the removal was actually needed
	        if(changed[j])
	        {
				// Mark old_node[j][0] and old_node[j][1] ( if needed ) as not live anymore.
				old_node[j][0]->live = 0;      
	            if (merge[j])
	                old_node[j][1]->live = 0;     
					
	            // Update the next pointers of the new node
	            i = 0;
			// If a merge is required, get the 'next' pointers from old_node[j][1] into the new node 'n'. Later on if old_node[j][0] has a higher max level than old_node[j][1], node 'n' will get the 'next' pointers from it.
			// If no merge is required, copy all next pointers from old_node[j][0] into the new node 'n'.  
	            if (merge[j])
	            {   
	                for (; i < old_node[j][1]->level; i++)
	                    n[j]->next[i] = old_node[j][1]->next[i];
	            }
	            for (; i < old_node[j][0]->level; i++)
	                n[j]->next[i] = old_node[j][0]->next[i];
				if (merge[j])	
				{
					// Link the predecessors 'next' pointers of old_node[0] ( in case there's a merge and old_node[1]->level > old_node[0]->level old_node[1]'s preds will point to n[j] as well )
		            //			to the new node n[j].  Notice that when linking predeccessors of old_node[j][1], preds_node1 is used.
					for(i = 0; i < old_node[j][0]->level; i++)
		            		{   
		               		 preds[j][i]->next[i] = n[j];
		            		}
					for(; i < old_node[j][1]->level; i++)
		            		{   
		               		 preds_node1[j][i]->next[i] = n[j];
		            		}
				}
				else
				{
            		for(i = 0; i < n[j]->level; i++)
            		{   
               		 preds[j][i]->next[i] = n[j];
            		}
				}
				// Node is fully linked to the list, set it as live.
	            n[j]->live = 1;

				// Unlock old_node[j][0],old_node[j][1] ( if needed) and all of their predeccessors 
	            if(merge[j])
				{
					pthread_mutex_unlock(&old_node[j][1]->lock);
	               
	            }
				pthread_mutex_unlock(&old_node[j][0]->lock);

				unlockPredForRemove(preds[j] ,preds_node1[j] ,highestLocked, old_node[j][0]->level );

				// Deallocate unused nodes.
				 if(merge[j])
				{
					 deallocate_node(old_node[j][1], ptst);
				}
	            deallocate_node(old_node[j][0], ptst);
		
	        }
	        else
	        {
				// No change is needed - deallocate new assigned node.
	            deallocate_node(n[j], ptst);
	        }    

		 /******** Release and Update end **********/
     	 }
    	 else
    	 {
		fail_merge:
			// Destroy the locks of the new nodes and the newly created tries.
     		pthread_mutex_destroy(&n[j]->lock);
			
			#ifdef	USE_TRIE
				trie_destroy(&n[j]->trie, ptst);
			#endif	/* !USE_TRIE */
		
		goto retry_remove;

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
    int i;
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
    ptst_t *ptst;

    low = low+ SENTINEL_DIFF; // Avoid sentinel
    high = high+ SENTINEL_DIFF;

    ptst = critical_enter();

retry_rq:
	// Used here just to locate the node that low is supposed to be in.
    n = search_predecessors(l, low, 0, 0);

	// The goal is to traverse on all nodes without any changes in the way in order to capture a correct snapshot of these set of nodes
	// Therefore, if one of the nodes in the way is discovered as not live anymore,  restart the entire search.
 
    while(high>n->high)
    {
        if (!n->live)
            goto retry_rq;
        n = n->next[0];
    }

    critical_exit(ptst);

    return 0;


}
void _init_set_subsystem(void)
{
    gc_id =     gc_add_allocator(sizeof(node_t));
#ifdef	USE_TRIE
    _init_trie_subsystem();
#endif	/* USE_TRIE */
}

