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
#include <stdio.h>

#include <string.h>
#include <assert.h>
#include "portable_defns.h"
#include "gc.h"
#include "stm.h"

#include "set.h"
#include <immintrin.h>
//#include rtm.h



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
#define mark_abo(_p) if(is_marked_ref(_p)) _xabort (0xff);
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
	printf("Print set place holder");
	/*
    node_t *cur = l;
    printf("Set nodes:\n");

    while (cur != NULL)
    {
        print_node2(cur, "\t"); 
        __transaction_atomic
        {
            cur = cur->next[0];
        }
    }*/
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
restart_look:
    {
        x = l;

        for ( i = MAX_LEVEL - 1; i >= 0; i-- )
        {
            for ( ; ; )
            {
                x_next = x->next[i];
                if (is_marked_ref(x_next) || (!x_next->live))
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


int removeAct(volatile node_t **old_node, node_t *n, setkey_t k, int merge, ptst_t *ptst)
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

setval_t set_update(set_t *l, setkey_t k, setval_t v, int overwrite)
{
    ptst_t   *ptst;
    volatile node_t *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *n[MAX_LEVEL];
    int j, i, indicator = 0, changed[MAX_ROW], split[MAX_ROW],status = 89;
    unsigned long max_height[MAX_ROW];
    node_t *new_node[MAX_ROW][2];
    unsigned long cnttt = 0;
    k=k+2; // Avoid sentinel

    ptst = critical_enter();

    for(j = 0; j<MAX_ROW; j++)
    {
        new_node[j][0] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(new_node[j][0]);
        new_node[j][1] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(new_node[j][1]);
        new_node[j][0]->live = 0;
        new_node[j][1]->live = 0;
    
retry_update:

   

#ifdef	USE_TRIE
        init_node_trie(new_node[j][0]);
        init_node_trie(new_node[j][1]);
#endif	/* USE_TRIE */

        n[j] = search_predecessors(db[j], k, preds[j], succs[j]);
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



   status = _xbegin()  ;
    if ( status == _XBEGIN_STARTED )
    { 
        
            if (n[j]->live == 0)
            { 
		            _xabort (5);
					goto fail_path;
            }

            for(i = 0; i < n[j]->level; i++)
            {   
                if(preds[j][i]->next[i] != n[j])
				{ 
			        _xabort (5);
					goto fail_path;
            	}	
                if(n[j]->next[i]) if(!n[j]->next[i]->live)
				{ 
			        _xabort (5);
					goto fail_path;
            	}
            }

            for(i = 0; i < max_height[j]; i++)
            {   
                if(preds[j][i]->next[i] != succs[j][i])
				{ 
		            _xabort (5);
					goto fail_path;
            	}
                if(!(preds[j][i]->live)) 
				{ 
		            _xabort (5);
					goto fail_path;
            	}
                if(!(succs[j][i]->live))
				{
 
		            _xabort (5);
					goto fail_path;
            	}
            }



            if(changed[j]) // lock
            {
                for(i = 0; i < n[j]->level; i++)
                {
                    if (n[j]->next[i] != NULL)
                    {
					   if(is_marked_ref(n[j]->next[i]))
					   {
			              	_xabort (5);
							goto fail_path;
                        }
			 
                        MARK(n[j]->next[i]);
                    }
                }                        

                for(i = 0; i < max_height[j]; i++)
                {
			       if(is_marked_ref(preds[j][i]->next[i]))
				   {
						printf("abort 8 ");
					
			            _xabort (5);
					
						goto fail_path;
	               }
                   MARK(preds[j][i]->next[i]);
                }

                n[j]->live = 0;
          }
		  if (_xtest())
		  {
			_xend();
		  }
    }
    else// else, transaction failed.
    {
	fail_path:
	#ifdef	USE_TRIE
	            /* deallocate the tries */
	            trie_destroy(&new_node[j][0]->trie, ptst);
	            if (split[j]) trie_destroy(&new_node[j][1]->trie, ptst);
	#endif	/* USE_TRIE */
        
    goto retry_update;
    }


   
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
                        new_node[j][1]->next[i] = get_unmarked_ref(n[j]->next[i]);
                    }
                    for (; i < new_node[j][1]->level; i++)
                        new_node[j][1]->next[i] = get_unmarked_ref(n[j]->next[i]);
                }
                else
                {   
                    for (i = 0; i < new_node[j][1]->level; i++)
                    {
                        new_node[j][0]->next[i] = new_node[j][1];
                        new_node[j][1]->next[i] = get_unmarked_ref(n[j]->next[i]);
                    }
                    for (; i < new_node[j][0]->level; i++)
                        new_node[j][0]->next[i] = succs[j][i];
                }
            }
            else
            {
                for (i = 0; i < new_node[j][0]->level; i++)
                {
                    new_node[j][0]->next[i] = get_unmarked_ref(n[j]->next[i]);
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
                new_node[j][1]->live = 1;
        


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
    }
    critical_exit(ptst);
    return 0;
}



setval_t set_remove(set_t *l, setkey_t k)
{
    ptst_t *ptst;
    volatile node_t *preds[MAX_ROW][MAX_LEVEL], *succs[MAX_ROW][MAX_LEVEL], *old_node[MAX_ROW][2];
    int i, j, total[MAX_ROW], indicator = 0, changed[MAX_ROW], merge[MAX_ROW];
    int indicator2 = 0;
    node_t *n[MAX_ROW];
    k=k+2; // Avoid sentinel

    ptst = critical_enter();
    for(j=0; j<MAX_ROW; j++)
    {
        n[j] = (node_t *) gc_alloc(ptst, gc_id);
        ASSERT_GC(n[j]);
    
retry_remove:
    
#ifdef	USE_TRIE
        init_node_trie(n[j]);
#endif	/* USE_TRIE */
    

retry_last_remove:
        merge[j] = 0;
        old_node[j][0] = search_predecessors(db[j], k, preds[j], succs[j]);

        /* If the key is not present, just return */
        if (find(old_node[j][0], k) == 0)
        {
            changed[j] = 0;
            continue;
        }

        do
        {
            old_node[j][1] = old_node[j][0]->next[0];
            if (!old_node[j][0]->live)
                goto retry_last_remove;
        //} while ((old_node[j][0]->live) && (is_marked_ref(old_node[j][1])));
        } while (is_marked_ref(old_node[j][1]));

        if (!old_node[j][0]->live)
            goto retry_last_remove;

        total[j] = old_node[j][0]->count;

        if(old_node[j][1])
        {
            total[j] = total[j] + old_node[j][1]->count;

            if (!old_node[j][0]->live || !old_node[j][1]->live)
                goto retry_last_remove;

            if(total[j] <= NODE_SIZE)
            {
                merge[j] = 1; 
            }
        }
        n[j]->level = old_node[j][0]->level;    
        n[j]->low   = old_node[j][0]->low;
        n[j]->count = old_node[j][0]->count;
        n[j]->live = 0;

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

        if (!old_node[j][0]->live)
            goto retry_last_remove;

        if (merge[j] && !old_node[j][1]->live)
            goto retry_last_remove;

        changed[j] = removeAct(old_node[j], n[j], k, merge[j], ptst);

    

    if (_xbegin()  == _XBEGIN_STARTED )
    {
        for(j=0; j<MAX_ROW; j++)
        {
            if(changed[j])
            {
                if (!old_node[j][0]->live)
                    _xabort (5);

                if (merge[j] && !old_node[j][1]->live)
                    _xabort (5);

                for(i = 0; i < old_node[j][0]->level;i++)
                {
                    if (preds[j][i]->next[i] != old_node[j][0])  _xabort (5);
                    if (!(preds[j][i]->live))  _xabort (5);
                    if (old_node[j][0]->next[i]) if (!old_node[j][0]->next[i]->live) _xabort (5);
                }


                if (merge[j])
                {   
                    // Already checked that old_node[0]->next[0] is live, need to check if they are still connected
                    if (old_node[j][0]->next[0] != old_node[j][1])
                        _xabort (5);

                    if (old_node[j][1]->level > old_node[j][0]->level)
                    {   
                        // Up to old_node[0] height, we only need to validate the next nodes of old_node[1]
                        for (i = 0; i < old_node[j][0]->level; i++)
                        {
                            if (old_node[j][1]->next[i]) if (!old_node[j][1]->next[i]->live)  _xabort (5);
                        }
                        // For the higher part, we need to check also the preds of that part
                        for (; i < old_node[j][1]->level; i++)
                        {
                            if (preds[j][i]->next[i] != old_node[j][1])  _xabort (5);
                            if (!(preds[j][i]->live))  _xabort (5);
                            if (old_node[j][1]->next[i]) if (!old_node[j][1]->next[i]->live) _xabort (5);
                        }

                    }
                    else // old_node[0] is higher than old_node[1], just check the next pointers of old_node[1]
                    {
                        for (i = 0; i < old_node[j][1]->level; i++)
                        {
                            if (old_node[j][1]->next[i]) if (!old_node[j][1]->next[i]->live)  _xabort (5);
                        }
                    }
                }

                // Lock the pointers to the next nodes
                if(merge[j])
                {
                    for(i = 0; i < old_node[j][1]->level; i++)
                    {
                        if (old_node[j][1]->next[i] != NULL)
                        {   
                            mark_abo(old_node[j][1]->next[i]);
                            MARK(old_node[j][1]->next[i]);
                        }
                    }
                    for(i = 0; i < old_node[j][0]->level; i++)
                    {
                        if (old_node[j][0]->next[i] != NULL)
                        {   
                            mark_abo(old_node[j][0]->next[i]);
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
                            MARK(old_node[j][0]->next[i]);
                        }
                    }
                }

                // Lock the pointers to the current node
                for(i = 0; i < n[j]->level; i++)
                {
                    mark_abo(preds[j][i]->next[i]);
                    MARK(preds[j][i]->next[i]);
                }

                old_node[j][0]->live = 0;      
                if (merge[j])
                    old_node[j][1]->live = 0;      
            }
        }
		if (_xtest())
		{
			_xend();
		}
        //indicator = 1;
    }
    else
    {
        
#ifdef	USE_TRIE
            trie_destroy(&n[j]->trie, ptst);
#endif	/* USE_TRIE */
        
        goto retry_remove;
    }

    
        if(changed[j])
        {

            // Update the next pointers of the new node
            i = 0;
            if (merge[j])
            {   
                for (; i < old_node[j][1]->level; i++)
                    n[j]->next[i] = get_unmarked_ref(old_node[j][1]->next[i]);
            }
            for (; i < old_node[j][0]->level; i++)
                n[j]->next[i] = get_unmarked_ref(old_node[j][0]->next[i]);

            for(i = 0; i < n[j]->level; i++)
            {   
                preds[j][i]->next[i] = n[j];
            }

            n[j]->live = 1;

            if(merge[j])
                deallocate_node(old_node[j][1], ptst);

            deallocate_node(old_node[j][0], ptst);
        }

        else
        {
            deallocate_node(n[j], ptst);
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
    int i, indicator = 0;
    ptst_t *ptst;

    low = low+2; // Avoid sentinel
    high = high+2;

    ptst = critical_enter();

retry_rq:
    n = search_predecessors(l, low, 0, 0);

    if (_xbegin()  == _XBEGIN_STARTED )
    {
        while(high>n->high)
        {
            if (!n->live)
                _xabort (5);
            n = get_unmarked_ref(n->next[0]);
        }
        //indicator = 1;
		if (_xtest())
		{
			_xend();
		}
    }
	else//    if(!indicator)
	{
        goto retry_rq;
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

