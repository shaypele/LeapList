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
	int counter = 0;
	
restart_look:

    {
	counter++;
        x = l;

        for ( i = MAX_LEVEL - 1; i >= 0; i-- )
        {
            for ( ; ; )
            { 
                x_next = x->next[i];
                if (!x_next->live)
                {
                    goto restart_look;
                }

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

set_t *set_alloc(void)
{
    ptst_t  *ptst;
    node_t *leap, *l;
    int i, j;

    ptst        = critical_enter();
    for(j=0; j<MAX_ROW; j++)
    {
        l           = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(l);
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
    return db[0];
}

setval_t find(volatile node_t *n, setkey_t k)
{
    int i;

    if(n)
    {
#ifdef	USE_TRIE
        if (n->count > 0)
        {
            trie_val_t res = trie_find_val(&n->trie, k);
            if (res != TRIE_KEY_NOT_FOUND)
            {
                return n->data[res].value;
            }

        }
#else	/* !USE_TRIE */
        for(i=0; i<n->count; i++)
        {
            if(n->data[i].key == k)
                return n->data[i].value;
        }
#endif	/* !USE_TRIE */

    }
    return 0;
}


int remove(volatile node_t **old_node, node_t *n, setkey_t k, int merge, ptst_t *ptst)
{
    int i,j;
    int changed = 0;

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
    trie_create_from_array(&n->trie, n->data, n->count, ptst);
#endif	/* USE_TRIE */
    return changed;
}

int insert(node_t **new_node,  volatile node_t *n, setkey_t k, setval_t v, int overwrite, int split, ptst_t *ptst)
{
    int i=0, j=0, changed = 0, m = 0;

    static int cnt = 0;

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
            if(n->data[j].key == k)
            {
                if(overwrite)
                {
					new_node[m]->data[i].key = n->data[j].key;
                    new_node[m]->data[i].value = v;
                    changed = 1;
#ifdef	USE_TRIE
                    /* need to use the same trie, so just copy it */
                    /* The following is removes because the trie will be created at the end of the function
                       new_node[m]->trie = n->trie;
                     */
#endif	/* USE_TRIE */
                }
                else 
                {
                    exit(9);
                    break;
                }
            }
			else
			{

	            if((!changed) && (n->data[j].key > k))
	            {
	                new_node[m]->data[i].key = k;
	                new_node[m]->data[i].value = v;
	                new_node[m]->count++;
	                changed = 1;

	                if((!m) && split && (new_node[0]->count == (i+1)))
	                {
	                    new_node[m]->high =  new_node[m+1]->low = new_node[m]->data[i].key;
	                    i = -1;
	                    m = m + 1;
	                }

	                i++;
	            }
	            new_node[m]->data[i].key = n->data[j].key;
	            new_node[m]->data[i].value = n->data[j].value;
			}
            if((!m) && split && (new_node[0]->count == (i+1)))
            {
                new_node[m]->high =  new_node[m+1]->low = new_node[m]->data[i].key;
                i = -1;
                m = m + 1;
            }

        }

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
        else //if (new_val)
        {
            /* build the trie for the new node */
            trie_create_from_array(&new_node[m]->trie, new_node[m]->data, new_node[m]->count, ptst);
        }
#endif	/* USE_TRIE */

    }
    return changed;
}


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

setval_t set_update(set_t *l, setkey_t k, setval_t v, int overwrite)
{
    ptst_t   *ptst;
    volatile node_t volatile *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *n[MAX_ROW];
    int j, i, indicator = 0, changed[MAX_ROW], split[MAX_ROW];
    unsigned long max_height[MAX_ROW];
    node_t *new_node[MAX_ROW][2];
    unsigned long cnttt = 0;
    k=k+2; // Avoid sentinel
    int level;
	node_t* lastLockedNode = 0x0;

	int isMarked, highestLocked = -1, valid = 1;
	node_t *pred = 0x0,*succ = 0x0,*prevPred = 0x0;

    ptst = critical_enter();
    for(j = 0; j<MAX_ROW; j++)
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
	int c = 0;
	for(j = 0; j<MAX_ROW; j++)
    {
		lastLockedNode  = 0x0;
		isMarked = 0;
	retry_update:
	c++;
				
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
#ifdef	USE_TRIE
        init_node_trie(new_node[j][0]);
		
        init_node_trie(new_node[j][1]);
		
#endif	/* USE_TRIE */

        n[j] = search_predecessors(db[j], k, preds[j], succs[j]);

		if (lastLockedNode != 0x0 && lastLockedNode != n[j]){
			if (lastLockedNode->isMarked){
				pthread_mutex_unlock(&lastLockedNode->lock);
				lastLockedNode->isMarked = 0;
				isMarked = 0;
			}
		}
		if ( isMarked || (!n[j]->isMarked && n[j]->live )  )
		{

			if ( !isMarked )
			{
				int err = 0,isLockFailed = 0;
				while ((err = pthread_mutex_trylock(&n[j]->lock)) && !isLockFailed){
					if (err == EINVAL){ // not initialized , so probably not live
						isLockFailed = 1;
					}
				}
				if (n[j]->isMarked || !n[j]->live){
					
					if (!isLockFailed){
						pthread_mutex_unlock(&n[j]->lock);
					}
					 
					 pthread_mutex_destroy(&new_node[j][0]->lock);
					 pthread_mutex_destroy(&new_node[j][1]->lock); 
					//Try to lock and is marked, then release lock and try again.
					#ifdef	USE_TRIE
			            // deallocate the tries  
			            trie_destroy(&new_node[j][0]->trie, ptst);
						
			            if (split[j]) trie_destroy(&new_node[j][1]->trie, ptst);
						
					#endif	// USE_TRIE 
					goto retry_update;
				}
				lastLockedNode = n[j];
				n[j]->isMarked = 1;
				isMarked = 1;
			}

	        if(n[j]->count == NODE_SIZE)
	        {
	            split[j] = 1; 
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

	        changed[j] = insert(new_node[j], n[j], k, v, overwrite, split[j], ptst);

			for ( level = 0; valid && ( level < max_height[j] ); level++){
				pred = preds[j][level];
				succ = succs[j][level];
				if (pred != prevPred){  
						int countLock2 = 0, err2 = 0;
						while (err2 = pthread_mutex_trylock(&pred->lock)){
							countLock2 ++;
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
				valid = !pred->isMarked && pred->next[level] == succ ;
			}
			if (!valid){
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


	    n[j]->live = 0;
	        if(changed[j]) // unlock
	        {
	            // Make the correct linking of the new nodes
	            if (split[j])
	            {   
	                if (new_node[j][1]->level > new_node[j][0]->level)
	                {   
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
	                for (i = 0; i < new_node[j][0]->level; i++)
	                {
	                    new_node[j][0]->next[i] = (n[j]->next[i]);
	                }
	            }
	            // Unlock the predecessors to the new nodes
	            for(i=0; i < new_node[j][0]->level; i++)
	            {
	                preds[j][i]->next[i] = new_node[j][0];
	            }
	            if (split[j] && (new_node[j][1]->level > new_node[j][0]->level))
	                for(; i < new_node[j][1]->level; i++)
	                {
	                    preds[j][i]->next[i] = new_node[j][1];
	                }


				
			new_node[j][0]->live = 1;

	            if (split[j])
	            {
	                			new_node[j][1]->live = 1;

	            }
			
	        }
 
			pthread_mutex_unlock(&n[j]->lock); 
			unlockPredForUpdate( preds[j], highestLocked);
			
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
    return 0;
}

void unlockPredForRemove(volatile  node_t **pa,volatile  node_t **pa_Node1 ,int highestLocked, int levelOldNode0 )
{
	node_t *prevPred = 0x0;
	 int iterateTill = -1;
     int level;

    	 if ( highestLocked < levelOldNode0 ){
    		 iterateTill = highestLocked; 
    	 }
    	 else{
    		 iterateTill = levelOldNode0 - 1;
    	 }
    	 // First unlock all pred of node 0->
		 for (level = 0 ; level <= iterateTill ; level++ ){
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
				if ( pa_Node1[level]!=prevPred )
				{
					pthread_mutex_unlock(&pa_Node1[level]->lock);
					prevPred = pa_Node1[level];
				}
			}
		}



	
}

setval_t set_remove(set_t *l, setkey_t k)
{
    ptst_t *ptst;
    volatile node_t volatile *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *old_node[MAX_ROW][2];
	volatile node_t volatile *preds_node1[MAX_ROW][MAX_LEVEL], *succs_node1[MAX_ROW][MAX_LEVEL];
	volatile node_t *pred, *succ, *prevPred = 0x0;   
    int i, j,  indicator = 0, changed[MAX_ROW], merge[MAX_ROW];
    int indicator2 = 0,valid = 1;
    node_t *n[MAX_ROW];
	node_t* lastLockedNode[2];
    k=k+2; // Avoid sentinel

    ptst = critical_enter();
    for(j=0; j<MAX_ROW; j++)
    {
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
    

    
retry_last_remove:
        merge[j] = 0;
        old_node[j][0] = search_predecessors(db[j], k, preds[j], succs[j]);
		
        /* If the key is not present, just return */
        if (find(old_node[j][0], k) == 0)
        {
			if (!old_node[j][0]->live){
        		 goto retry_last_remove;
        	 }
            changed[j] = 0;
			
			#ifdef	USE_TRIE
				trie_destroy(&n[j]->trie, ptst);
			#endif	/* !USE_TRIE */
			
			old_node[j][1] = old_node[j][0]->next[0];
			if (old_node[j][0]->isMarked && isMarkedArr[0]){
					pthread_mutex_unlock(&old_node[j][0]->lock);
				old_node[j][0]->isMarked = 0;
				isMarkedArr[0] = 0;
				}
				
				if (old_node[j][1]!=0 && old_node[j][1]->isMarked && isMarkedArr[1]){
					pthread_mutex_unlock(&old_node[j][1]->lock);
				old_node[j][1]->isMarked = 0;
				isMarkedArr[1] = 0;
				}
            continue;
        }

		if (lastLockedNode[0] != 0x0 && lastLockedNode[0] != old_node[j][0]){
			if (lastLockedNode[0]->isMarked){
				pthread_mutex_unlock(&lastLockedNode[0]->lock);
			lastLockedNode[0]->isMarked = 0;
			isMarkedArr[0] = 0;
			};
		}
		
		old_node[j][1] = old_node[j][0]->next[0];
		if (old_node[j][1]!= 0 && 
         		(old_node[j][0]->count + old_node[j][1]->count - 1) <= NODE_SIZE - 20 ) 
		{
         		merge[j] = 1;
		}
		else
		{
			merge[j] = 0;
		}
		
		// if node is marked and not by this thread try to search the node once more.
    	 if ( 	(isMarkedArr[0] || 
    			(  !old_node[j][0]->isMarked &&  old_node[j][0]->live ))  ){
				// if node is marked and not by this thread try to search the node once more.
				if (merge[j] && !(isMarkedArr[1] || 
										(  !old_node[j][1]->isMarked &&  old_node[j][1]->live ))  )
				{
					goto fail_merge;
				}
				
				
    		 if (!merge[j]){
				 if (!isMarkedArr[0]){

					int countLock = 0, err = 0,isLockFailed = 0;
					while ((err = pthread_mutex_trylock(&old_node[j][0]->lock)) && !isLockFailed){
						if (err == EINVAL){
								isLockFailed = 1;
							}
					}
					
					
					 if (old_node[j][0]->isMarked || !old_node[j][0]->live ){
						if (!isLockFailed)
						{
							pthread_mutex_unlock(&old_node[j][0]->lock);
						}

						#ifdef	USE_TRIE
							trie_destroy(&n[j]->trie, ptst);
						#endif	/* !USE_TRIE */
						
						pthread_mutex_destroy(&n[j]->lock);
						 goto retry_remove;
					 }
					 lastLockedNode[0] = old_node[j][0];
					 old_node[j][0]->isMarked = 1;
					 isMarkedArr[0] = 1;
				}
    		}
			//merge , lock backwards
			else{ 
				if (lastLockedNode[1] != 0x0 && lastLockedNode[1] != old_node[j][1]){
					if (lastLockedNode[1]->isMarked){
						pthread_mutex_unlock(&lastLockedNode[1]->lock);
					lastLockedNode[1]->isMarked = 0;
					isMarkedArr[1] = 0;
					}
				}
				
				// Mark and lock second node
				if (!isMarkedArr[0]){
					if (!isMarkedArr[1]){												 
						int countLock = 0, err = 0,isLockFailed = 0;
						// if couldn't acquire lock because of initialization issues , try remove again.
						while ((err = pthread_mutex_trylock(&old_node[j][1]->lock)) && !isLockFailed){
							if (err == EINVAL){
									isLockFailed = 1;
								}
						}
						
						
						 if (old_node[j][1]->isMarked || !old_node[j][1]->live ){
							if (!isLockFailed)
							{
								pthread_mutex_unlock(&old_node[j][1]->lock);
							}
							
							#ifdef	USE_TRIE
								trie_destroy(&n[j]->trie, ptst);
							#endif	/* !USE_TRIE */
							
							pthread_mutex_destroy(&n[j]->lock);
							 goto retry_remove;
						 }
						 lastLockedNode[1] = old_node[j][1];
						 old_node[j][1]->isMarked = 1;
						 isMarkedArr[1] = 1;	


					}
					
					int err4 = 0,isLockFailed2 = 0;
					while ((err4 = pthread_mutex_trylock(&old_node[j][0]->lock)) && !isLockFailed2){
						if (err4 == EINVAL){
								isLockFailed2 = 1;
							}
					}
				
    			
					 if (old_node[j][0]->isMarked || !old_node[j][0]->live ){
						if (!isLockFailed2)
						{
							pthread_mutex_unlock(&old_node[j][0]->lock);
						}

						#ifdef	USE_TRIE
							trie_destroy(&n[j]->trie, ptst);
						#endif	/* !USE_TRIE */
						
						
						pthread_mutex_destroy(&n[j]->lock);
						 goto retry_remove;
					 }
					 lastLockedNode[0] = old_node[j][0];
					 old_node[j][0]->isMarked = 1;
					 isMarkedArr[0] = 1; 
				}
         	}

			 /********* Remove Setup ***********/
	        n[j]->level = old_node[j][0]->level;    
	        n[j]->low   = old_node[j][0]->low;
	        n[j]->count = old_node[j][0]->count;
	        n[j]->live = 0;
			n[j]->isMarked = 0;
			

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
			
	        changed[j] = remove(old_node[j], n[j], k, merge[j], ptst);
			/****** End Remove Setup ***********/

			
         	//first, lock all prevs of node 0-> Later on, if needed, lock preds of node 1->
         	int level;
			
         	for ( level = 0; valid && ( level < old_node[j][0]->level ); level++){
				pred = preds[j][level];
				succ = succs[j][level];
				if (pred != prevPred){

					int err2 = 0;
					while (err2 = pthread_mutex_trylock(&pred->lock)){
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
				valid = !pred->isMarked && pred->next[level] == succ;
			}
			if (!valid){
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
         		// Find preds of node 1->
				search_predecessors(db[j], old_node[j][1]->high, preds_node1[j], succs_node1[j]);
         		for(; valid && ( level < old_node[j][1]->level ); level++){
         			pred = preds_node1[j][level];
					succ = succs_node1[j][level];
					if (pred != prevPred){

					int err3 = 0;
					while (err3 = pthread_mutex_trylock(&pred->lock)){
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
					valid = !pred->isMarked && pred->next[level] == succ;
				}
				if (!valid){
					unlockPredForRemove(preds[j] ,preds_node1[j] ,highestLocked, old_node[j][0]->level );
					
					#ifdef	USE_TRIE
						trie_destroy(&n[j]->trie, ptst);
					#endif	/* !USE_TRIE */
					
					pthread_mutex_destroy(&n[j]->lock);
					goto retry_remove;
				}	
         	}
         	
         	/******** Release and Update **********/
			
	        if(changed[j])
	        {
				old_node[j][0]->live = 0;      
	            if (merge[j])
	                old_node[j][1]->live = 0;     
					
	            // Update the next pointers of the new node
	            i = 0;
	            if (merge[j])
	            {   
	                for (; i < old_node[j][1]->level; i++)
	                    n[j]->next[i] = old_node[j][1]->next[i];
	            }
	            for (; i < old_node[j][0]->level; i++)
	                n[j]->next[i] = old_node[j][0]->next[i];
			if (merge[j])	
			{
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
	            n[j]->live = 1;

				// Unlock
	            if(merge[j])
				{
					pthread_mutex_unlock(&old_node[j][1]->lock);
	               
	            }
				pthread_mutex_unlock(&old_node[j][0]->lock);

				unlockPredForRemove(preds[j] ,preds_node1[j] ,highestLocked, old_node[j][0]->level );

				//Deallocate
				 if(merge[j])
				{
					 deallocate_node(old_node[j][1], ptst);
				}
	            deallocate_node(old_node[j][0], ptst);
		
	        }
	        else
	        {
	            deallocate_node(n[j], ptst);
	        }    

		 /******** Release and Update end **********/
     	 }
    	 else
    	 {
		fail_merge:
     		pthread_mutex_destroy(&n[j]->lock);
			
			#ifdef	USE_TRIE
				trie_destroy(&n[j]->trie, ptst);
			#endif	/* !USE_TRIE */
		
		goto retry_remove;

    	 }    
    }

    critical_exit(ptst);
    return 0;
}



setval_t set_lookup(set_t *l, setkey_t k)
{
    volatile node_t *n;
    int i, indicator = 0;
    ptst_t *ptst;
    setval_t v;

    k=k+2; // Avoid sentinel

    ptst = critical_enter();

retry_lookup:
    n = search_predecessors(l, k, 0, 0);

    v = find(n,k);

    critical_exit(ptst);

    return v;
}

setval_t set_rq(set_t *l, setkey_t low, setkey_t high)
{
    volatile node_t *n;
    ptst_t *ptst;

    low = low+2; // Avoid sentinel
    high = high+2;

    ptst = critical_enter();

retry_rq:
    n = search_predecessors(l, low, 0, 0);

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

