package leapListReg;

import java.util.concurrent.ThreadLocalRandom;

import utils.Trie;


/* 

Created By:

Yossef Yakobi - yossi_ya@yahoo.com
Shay Peled - shaypele@gmail.com
David Meriin - meriind@yahoo.com

Parameter recommended settings :

 1. Use trie only in cases where the data structure is used for lookup or range query only ( or a very small percentage of modification ) .
 2. Maximum node size of 100 elements is recommended.
 3. Keep in mind that a big key range will affect performance.


*/

public class LeapListDB {
	int MAX_ROW;
	public volatile LeapList[] LeapLists;
	
	public LeapListDB (int numOfLists) {
		MAX_ROW = numOfLists;
		LeapLists = new LeapList[MAX_ROW];
		for (int i=0; i < MAX_ROW ; i++)
		{
			LeapLists[i] = new LeapList();
		}
	}
		
	/*
	 * Returns the list for the matching index.
	 */
	public LeapList  GetListByIndex (int index){
		if (index < MAX_ROW){
			return LeapLists[index];
		}
		else{
			return null;
		}
	}
	
	/*
	 * The method receives a List and a key.
	 * It calls the lookup method for the LeapList.
	 */
	public Object lookUp (LeapList l, long key){
		return l.lookUp(key);
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It updates the key[i] value[i] pair for LeapList[i], by calling updateSetup() and updateRelease().
	 */
	public void leapListUpdate (LeapList [] ll, long [] keys, Object [] values, int size){
		// Pa, na - filled by search_predeccessors
		LeapNode[] pa = null;
		LeapNode[] na = null;
		LeapNode n = null;
		LeapNode[] newNode = new LeapNode[2];
		int [] maxHeight = new int[size];
		long[] newKeys = new long[size];
		boolean [] split = new boolean [size];
		boolean [] changed = new boolean [size];
		
		for(int i = 0; i < size; i++){
			newKeys[i] = keys[i] + 2 ; // avoid sentinel; 
		}
		
		for (int i = 0 ; i < size; i++){
			pa = new LeapNode[LeapList.MAX_LEVEL];
			na = new LeapNode[LeapList.MAX_LEVEL];
			newNode[0] = new LeapNode();
			newNode[1] = new LeapNode();
			LeapNode lastLockedNode = null;
			// isMarked - true if node is marked by this thread, 0 otherwise. 
			boolean isMarked = false;
			while (true){
				// Get the predeccsors (in all of the levels ) of the node that newKeys[i] should be added into pa, get the successors of pa into na.
				// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node (or 2 new nodes is split is required), this node is returned
				//	into n.
				ll[i].searchPredecessor(newKeys[i], pa, na);	
				n = na[0];
				
				// In case there was a restart and the key now is under a different node, unlock & unmark previously locked node ( if needed ) 
				if (lastLockedNode != null && lastLockedNode != n){
					if (lastLockedNode.nodeLock.isLocked()){
						lastLockedNode.unlock();
					}
					lastLockedNode.Marked = false;
					isMarked = false;
				}
				// highestLocked - highest level locked.
				byte highestLocked = -1;
				try{
					LeapNode pred,succ, prevPred =null;
					boolean valid = true;
					// Continue with the update if the node that contains the key is marked by this thread or if the node is not marked and is live.
					// Otherwise retry opertation.
					if (isMarked ||
						(!n.Marked && n.live ) )
					{
						// If node is not marked yet by this thread, it needs to be locked and marked.
						if ( !isMarked ){
							n.lock();
							if (n.Marked){
								n.unlock();
								continue;
							}
							lastLockedNode = n;
							n.Marked = true;
							isMarked = true;
						}
						// Create new nodes and decide whether to split or not only after lock has been acquired
						updateSetup (ll[i], newKeys[i], values[i], n, newNode, maxHeight, split, changed, i);
						
						// Lock all predeccessors of newKeys[i] in order to remove the link and relink to the new node later.
						// Lock in an ascending order ( 0 to top level) in order to avoid deadlocks.
						// The following code would fail and restart the whold operation if one ( or more ) of these conditions occurs :
						//  a . Any of the locks fails 
						//  b. one of the predeccessors is marked to be removed
						//  c. one of the 'next' pointers of any of the predeccessors doesn't point to the expected node ( i.e. successor )
						// When any of these conditions occurs, a restart of the operation is in order. Before restarting unlock any predeccessors that were already locked. 
						// Also,destroy the newly created tries and the locks of the new nodes.
						
						
						for ( int level = 0; valid && ( level < maxHeight[i] ); level++){
							pred = pa[level];
							succ = na[level];
							// No need to lock twice if the same node is a predeccessor in a different level.
							if (pred != prevPred){
								pred.lock();
								highestLocked = (byte) level;
								prevPred = pred;
							}
							// Invalidate operation if the predeccessor is marked to be removed or doesn't point to the expected successor 
							valid = !pred.Marked && pred.getNext(level) == succ ;
						}
						if (!valid){
							continue;
						}	
						updateRelease (pa, na, n, newNode, split, changed, i);
						n.unlock();
						break;
					}
					else
					{
						continue;
					}
				}	
				finally{
		 			// Unlock all n's predeccessors 
					LeapNode prevPred = null;
					for (int j = 0 ; j <= highestLocked ; j++ ){
						if ( pa[j]!=prevPred )
						{
							pa[j].unlock();
							prevPred = pa[j];
						}
					}
				}
			}
		}	
		
	}

	/*
	 * The method uses ThreadLocalRandom to return a byte value with a 50% chance increment starting from 1,
	 */
	private byte getLevel(){
		long r = ThreadLocalRandom.current().nextLong();
		byte l = 1;
		r = (r >> 4) & ((1 << (LeapList.MAX_LEVEL - 1)) -1);
		while ((r & 1)  > 0){
			l++;
			r >>= 1;
		}
		return l;
	}
	
	/*
	 * The method checks if the matching Node is
	 * at its maximum size and if so splits it. It then calls the insert method to copy the values to the updated node.
	 */
	private void updateSetup (  LeapList l, long key, Object value,  
										LeapNode n, LeapNode [] newNode, int [] maxHeight, boolean[] split, boolean[] changed, int i){
		
		// If the node to be removed has reached its maximum size, it should be splitted into 2 new nodes.Otherwise only one node will replace it.
			if (n.count == LeapList.NODE_SIZE){
				split[i] = true;
				// When splitting, one node will have the same level as the old node, and the other will get a random level ( With drop-off rate of 0.5 per level.)
				newNode[1].level = n.level;
				newNode[0].level = getLevel();
				maxHeight[i] = (newNode[0].level > newNode[1].level) ? newNode[0].level: newNode[1].level;
			}
			else{
				split[i] = false;
				newNode[0].level = n.level;
				maxHeight[i] = newNode[0].level;
			}
			
			// Copy values of the old node to the new node ( or 2 if splitted ).
			changed [i] = insert(newNode, n, key, value, split[i]);
	}
	
	// called by set_update.
	// Gets an array of new_node (contains 2 in case there's a split) to set the new values in , node n to take the old key-value pairs from, key 'key' & value 'val' to insert/update,
	// 'split' - true to split/false not to split,
	// Return true if key-value pair was added/updated , false otherwise.
	private boolean insert (LeapNode[] newNode, LeapNode n, long key, Object val, boolean split){
		boolean changed = false;
		int m = 0;
		int i = 0;
		int j = 0;
	
		// If there's a split, put NODE_SIZE/2 elements in one node and the rest in the other.
		// The 2 new nodes are now associated with the key range that was assoicated with one node 'n'

		// Otherwise, copy old 'n' properties to the new node
		if (split){
			newNode[0].low = n.low;
			newNode[0].count = (LeapList.NODE_SIZE/2);
			newNode[1].high = n.high;
			newNode[1].count = n.count - (LeapList.NODE_SIZE/2);
		}
		else
		{
			newNode[0].low = n.low;
			newNode[0].high = n.high;
			newNode[0].count = n.count;
		}
		
		// Add/update the given key-value pair (key,val),also  add all elements from node 'n' to the new set of nodes (divide between 2 node if there's a split )
		if (n.count == 0){
			newNode[m].data[0].key = key;
			newNode[m].data[0].value = val;
			newNode[m].count = 1;
			changed = true;
			newNode[m].trie = new Trie(key, (short)0);
		}
		else{
			for (j = 0; j < n.count; i++, j++){
				
				// If key was found in old node update it's value and assign it to the new node.
				//Otherwise , add it like a normal key-value pair from node 'n'
				if (n.data[j].key == key){     
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = val;
					changed = true;
				}
				else
				{
					// Data is a sorted array therefore if next element has a bigger key then the given key 'key', that's is the place to put the given key-value pair into.
					if ((!changed) && (n.data[j].key > key)){
						newNode[m].data[i].key = key;
						newNode[m].data[i].value = val;
						newNode[m].count++;
						changed = true;
						
						// Move to next node if split == 1 and new_node has reached it's assigned capcity 					
						if ((m != 1) && split && (newNode[0].count == (i+1))){
							newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
							i = -1;
							m = m + 1;
						}
						
						i++;
					}
					
					//Copy elemetns from old node 'n' to the new node.
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = n.data[j].value;
				}
				
				// Move to next node if split == 1 and new_node has reached it's assigned capcity
				if ((m != 1) && split && (newNode[0].count == (i+1))){
					newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
					i = -1;
					m = m+1;
				}
			}
			
			// In case the given key-value pair should be inserted in the end of the array, insert it here.
			if (!changed){
				newNode[m].count++;
				newNode[m].data[i].key = key;
				newNode[m].data[i].value = val;
				changed = true;
			}
			
			if (split){
				/* build the tries for the new nodes */
				newNode[0].trie = new Trie(newNode[0].data, newNode[0].count);
				newNode[1].trie = new Trie(newNode[1].data, newNode[1].count);
			}
			else{
				/* build the trie for the new node */
				newNode[m].trie = new Trie(newNode[m].data, newNode[m].count);
			}
		}
		return changed;
	}
	
	/*
	 * The method inserts the new Node\s into the list by updating the relevant pointers using the predecessor and successor arrays.
	 */
	private void  updateRelease (LeapNode[] pa, LeapNode[] na, LeapNode n, LeapNode[] newNode, boolean[] split,
									boolean[] changed, int j){
			int i = 0;
			
			// Link the pointers of the new nodes to point to the successors in na. Also, re-link the predeccessors pointers to point to the new nodes assigned.
			// After that the new nodes are accessible, they are part of the lists, therefore they are set to be live.
			if (changed[j]){
				// First point the next pointers of all level in the new node ( or 2 if there's a split)  to the assoicated successors in succs.
				if (split[j]){
					// If there is a split, distinguish between two case: one where new_node[1]->level > new_node[0]->level and the opposite.
					if (newNode[1].level > newNode[0].level){
						//  Since new_node[1]->level = n->level and new_node[1]->level > new_node[0]->level, point all 'next' pointers in new_node[0] to new_node[1],
						//  Next, copy all 'next' pointers from n to new_node[1] (since they share the same maximum level)
						for (i = 0; i < newNode[0].level; i++){
							newNode[0].setNext(i, newNode[1]) ;
                            newNode[1].setNext(i, n.getNext(i));
						}
						 for (; i < newNode[1].level; i++)
	                            newNode[1].setNext(i,n.getNext(i));
					}
					else
                    {   
						// In this case only point part of new_node[0] 'next' pointers to new_node[1]. The others should point to na[i] , where i represents the higher levels.
						// This is because new_node[0] max level is bigger than the original node n's max level.
                        for (i = 0; i < newNode[1].level; i++)
                        {
                            newNode[0].setNext(i, newNode[1]);
                            newNode[1].setNext(i, n.getNext(i));
                        }
                        for (; i < newNode[0].level; i++){
                            newNode[0].setNext(i, na[i]);
                        }
                    }
				}
				else
                {
					// If no split occurred, simply copy all 'next' pointers from n to new_node[0].
                    for (i = 0; i < newNode[0].level; i++)
                    {
                        newNode[0].setNext(i, n.getNext(i));
                    }
                }
				
	            // Link the predecessors 'next' pointers of the node n to the new node ( or 2 if there's a split )
				for(i=0; i < newNode[0].level; i++)
                {
                    pa[i].setNext(i, newNode[0]);
                }
				
				// If there's a split and new_node[1]->level > new_node[0]->level , Link the predecessors 'next' pointers of the node n to new node[1]
                if (split[j] && (newNode[1].level > newNode[0].level)){
                    for(; i < newNode[1].level; i++)
                    { 	
                        pa[i].setNext(i, newNode[1]);
                    }
                }
                
				// Linking completed, mark the new nodes as live.	
                newNode[0].live = true;
                if (split[j]){
                	newNode[1].live = true;
                }
                n.live = false;
			}
		
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It removes the key[i] value[i] pair for LeapList[i] (if exists), by calling the searchPredecessor(),
	 * removeSetup() and removeRelease().
	 */
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
		// Pa and na are filled by search_predeccessors
	    LeapNode[]  pa = null;
	    LeapNode[] na = null;
	    LeapNode[] pa_Node1 = null;
 		LeapNode[] na_Node1 = null;
	    LeapNode n = null;
	    LeapNode[] oldNode = new LeapNode[2];
	    long[] newKeys = new long[size];
	    int j;
	    boolean[] changed = new boolean[size], merge = new boolean[size];
	   

	    for(j=0; j<size; j++)
	    {
	    	pa = new LeapNode[LeapList.MAX_LEVEL];
			na = new LeapNode[LeapList.MAX_LEVEL];
			pa_Node1 = new LeapNode[LeapList.MAX_LEVEL];
			na_Node1 = new LeapNode[LeapList.MAX_LEVEL];
	    	n = new LeapNode();
	        newKeys[j] = keys[j]+2; // Avoid sentinel
	        
	        boolean[] isMarkedArr = new boolean[2]; 
	        isMarkedArr[0] = false;
	        isMarkedArr[1] = false;
	        
	        while (true){
	        	// Get the predeccsors (in all of the levels ) of the node that newKeys[j] should be added to into pa, get the successors of pa into na.
	    		// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node, this node is returned
	    		//	into oldNode[0].
				 ll[j].searchPredecessor( newKeys[j], pa, na);
			     oldNode[0] = na[0];
			     byte highestLocked = -1;
			     
			     /* If the key is not present, just return 
			      * In case, a node was previously locked, unlock it*/
		         if (find(oldNode[0], newKeys[j]) == null)
		         {
		        	 if (!oldNode[0].live){
		        		 continue;
		        	 }
		        		 
		        	 changed[j] = false;
		        	 
		        	// Get the next node of oldNode[0] 
		        	 oldNode[1] = oldNode[0].getNext(0);
		        	 
		        	// In case this is a restart of the operation and oldNode[0] was already marked by this thread, but the key wasn't found anymore,
		 			// then unlock the node's lock and unmark it.
		 			if (oldNode[0].Marked && isMarkedArr[0]){
	 					oldNode[0].unlock();
		 				oldNode[0].Marked = false;
		 				isMarkedArr[0] = false;
		 			}
		 				
		 			// In case this is a restart of the operation and oldNode[1] was already marked by this thread, but the key wasn't found anymore,
		 			// then unlock the node's lock and unmark it.
	 				if (oldNode[1]!=null && oldNode[1].Marked && isMarkedArr[1]){
	 					oldNode[1].unlock();
		 				oldNode[1].Marked = false;
		 				isMarkedArr[1] = false;
	 				}
		            break;
		         }
		        
		        // Find out if it's a merge or not.
		         // Merge if it's there are less elements two nodes than the merge threshold.
				// Merge threshold was reduced here to NODE_SIZE - 10. When threshold was set to NODE_SIZE, there were cases were  a split and a merge could happen concurrently. 
				// This could get the application to get stuck, this way a safety margin of size 10 is taken in order to prevent that end case.
		        oldNode[1] = oldNode[0].getNext(0);
		 		if (oldNode[1]!= null && 
		          		(oldNode[0].count + oldNode[1].count - 1) <= LeapList.NODE_SIZE - 10 ) 
		 		{
		          		merge[j] = true;
		 		}
		 		else
		 		{
		 			merge[j] = false;
		 		}
		         
			     try{
			    	 LeapNode pred,succ, prevPred =null;
			    	 boolean valid = true;
			    	 
			    	// Continue with the remove if the node that contains the key is marked by this thread or if the node is not marked and is live.
			 		// Otherwise retry opertation.
			    	 if ( 	isMarkedArr[0] || 
			    			(  !oldNode[0].Marked &&  oldNode[0].live ) ){
							// if node is marked and not by this thread try to search the node once more.
							if (merge[j] && !(isMarkedArr[1] || 
													(  !oldNode[1].Marked &&  oldNode[1].live ))  )
							{
								continue;
							}
							
						// If no merge is decided, lock and mark only old_node[0]			
			    		if ( !merge[j] )
			    		{
			    			// If node is not marked yet by this thread, it needs to be locked and marked.
				    		if (!isMarkedArr[0])
				    		{
				    			 oldNode[0].lock();
				    			 if (oldNode[0].Marked){
				    				 oldNode[0].unlock();
				    				 continue;
				    			 }
				    			 oldNode[0].Marked = true;
				    			 isMarkedArr[0] = true;
				    		}
			    		}
			    		
			    		//merge , lock backwards . First Lock oldNode[1] and then oldNode[0]
						// This is done in order to avoid deadlocks and maintain the always lock backwards policiy ( as done with node locking and its predeccessors)
			    		else
			    		{
			    			// Mark and lock second node. It is enough to check if oldMode[0] was marked by this thread or not.
			    			if (!isMarkedArr[0])
				    		{
			    				// If node is not marked yet by this thread, it needs to be locked and marked.
			    				if (!isMarkedArr[1])
					    		{
					    			 oldNode[1].lock();
					    			 if (oldNode[1].Marked){
					    				 oldNode[1].unlock();
					    				 continue;
					    			 }
					    			 oldNode[1].Marked = true;
					    			 isMarkedArr[1] = true;
					    		}
			    				
			    				// Lock old node 0
			    				 oldNode[0].lock();
				    			 if (oldNode[0].Marked){
				    				 oldNode[0].unlock();
				    				 continue;
				    			 }
				    			 oldNode[0].Marked = true;
				    			 isMarkedArr[0] = true;
				    		}
			    		}
			    		 
			    		 
			    		 
			    		 
			         	// Mark and lock second node
			         	if (merge[j] && !isMarkedArr[1]){
			         		 if (!oldNode[1].tryLock()){
			         			oldNode[0].Marked = false;
				    			 isMarkedArr[0] = false;
			         			oldNode[0].unlock();
			         			 continue;
			         		 }
			    			 if (oldNode[1].Marked){
			    				 oldNode[1].unlock();
			    				 continue;
			    			 }
			    			 oldNode[1].Marked = true;
			    			 isMarkedArr[1] = true;
			         	}
			         	
			         	RemoveSetup(ll[j],newKeys[j], n, oldNode, merge, changed,j);
			         	//first, lock all prevs of node 0. Later of, if needed, lock preds of node 1.
			         	byte level;
			         	
						// Lock all predeccessors of old_node[0] in order to remove the link and relink to the new node later.
						// Lock in an ascending order ( 0 to top level) in order to avoid deadlocks.
						// The following code would fail and restart the whold operation if one ( or more ) of these conditions occurs :
						//  a . Any of the locks fails 
						//  b. one of the predeccessors is marked to be removed
						//  c. one of the 'next' pointers of any of the predeccessors doesn't point to the expected node ( i.e. successor )
						// When any of these conditions occurs, a restart of the operation is in order. Before restarting unlock any predeccessors that were already locked. 
						// Also,destroy the newly created tries and the locks of the new nodes.

						// If merge is needed and ( old_node[0]->level < old_node[1]->level ) do the same for old_node[1]'s predeccessors
			         	
			         	for ( level = 0; valid && ( level < oldNode[0].level ); level++){
							pred = pa[level];
							succ = na[level];
							
							// No need to lock twice if the same node is a predeccessor in a different level.
							if (pred != prevPred){
								pred.lock();
								highestLocked = level;
								prevPred = pred;
							}
							
							// Invalidate operation if the predeccessor is marked to be removed or doesn't point to the expected successor 
							valid = !pred.Marked && pred.getNext(level) == succ;
						}
						if (!valid){
							continue;
						}	
						
						// if node 1's level is bigger than node 0's level, lock preds of higher level of node 1. 
			         	if ( merge[j] && ( oldNode[0].level < oldNode[1].level ) ){
			         		// Find preds of node 1.
			         		ll[j].searchPredecessor(oldNode[1].high, pa_Node1, na_Node1 );
			         		for(; valid && ( level < oldNode[1].level ); level++){
			         			pred = pa_Node1[level];
								succ = na_Node1[level];
								
								// No need to lock twice if the same node is a predeccessor in a different level.
								if (pred != prevPred){
									pred.lock();
									highestLocked = level;
									prevPred = pred;
								}
								
								// Invalidate operation if the predeccessor is marked to be removed or doesn't point to the expected successor 
								valid = !pred.Marked && pred.getNext(level) == succ;
							}
							if (!valid){
								continue;
							}	
			         	}
			         	
			         	// Link the pointers of the new nodes to point to the successors in na. Also, re-link the predeccessors pointers to point to the new nodes assigned.
			    		// After that the new nodes are accessible, they are part of the lists, therefore they are set to be live.		
			         	RemoveReleaseAndUpdate(pa, na, n, oldNode, merge, changed, j, pa_Node1, na_Node1);
			         	oldNode[0].unlock();
			         	if (merge[j]){
			         		oldNode[1].unlock();
			         	}
			         	break;
			    	 }
			    	 else
			    	 {
			    		 continue;
			    	 }
			     }
			     finally{
			    	 
			    	 // unlock all predecessors of oldNode
			    	 
			    	 LeapNode prevPred = null;
			    	 int iterateTill = -1;
			    	 byte level;
			    	 
			    	 if ( highestLocked < oldNode[0].level ){
			    		 iterateTill = highestLocked; 
			    	 }
			    	 else{
			    		 iterateTill = oldNode[0].level - 1;
			    	 }
			    	 // First unlock all pred of node 0.
					 for (level = 0 ; level <= iterateTill ; level++ ){
						if ( pa[level]!=prevPred )
						{
							pa[level].unlock();
							prevPred = pa[level];
						}
					 }
					 
					 // if needed , unlock rest of preds of node 1.
					if ( highestLocked >= oldNode[0].level ){
						iterateTill = highestLocked; 
						for (; level <= iterateTill ; level++ ){
							if ( pa_Node1[level]!=prevPred )
							{
								pa_Node1[level].unlock();
								prevPred = pa_Node1[level];
							}
						}
					}
			     }
	        }
	        
		    
		  
		    
	    }
	    
	}

	/*
	 * The method removes the Node from the list by updating the relevant pointers using the predecessor and successor arrays.
	 */
	private void RemoveReleaseAndUpdate(LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int j,LeapNode[] pa_Node1,LeapNode[] na_Node1) {
		 
			// update links only if the removal was actually needed
	        if(changed[j])
	        {
	        	
				// Mark old_node[0] and old_node[1] ( if needed ) as not live anymore.
	        	if(merge[j])
	            {
	            	oldNode[1].live = false;
	            	oldNode[1].trie=null;
	            }

	            oldNode[0].live = false;
	            oldNode[0].trie=null;
	        	
	            // Update the next pointers of the new node
	            int i=0;
	            
				// If a merge is required, get the 'next' pointers from old_node[1] into the new node 'n'. Later on if old_node[0] has a higher max level than old_node[1], node 'n' will get the 'next' pointers from it.
				// If no merge is required, copy all next pointers from old_node[0] into the new node 'n'.  
	            if (merge[j])
	            {   
	                for (; i < oldNode[1].level; i++)
	                    n.setNext(i, oldNode[1].getNext(i));
	            }
	            for (; i < oldNode[0].level; i++)
	                n.setNext(i, oldNode[0].getNext(i));
	            
	            
	            // Link the predecessors 'next' pointers of oldNode[0] ( in case there's a merge and oldNode[1]->level > oldNode[0]->level oldNode[1]'s preds will point to n as well )
	            //			to the new node n.  Notice that when linking predeccessors of old_node[j][1], preds_node1 is used.
	            for(i = 0; i < oldNode[0].level; i++)
	            {   
	                pa[i].setNext(i, n);
	            }
	            
	            if ( merge[j] && ( oldNode[0].level < oldNode[1].level ) ){
	            	for(; i < oldNode[1].level; i++)
		            {  
	            		pa_Node1[i].setNext(i, n);
		            }
	            }
	            
	         // Node is fully linked to the list, set it as live.
	            n.live = true;
	          
	        }
	        else
	        {
	            n.trie=null;
	        } 
	}
	
	/*
	 * The method searches for a key in a given Node and return the matching object if found. Return null if not.
	 */
	private Object find(LeapNode node,long key){
		
		if(node!=null && node.live){
			if (node.count > 0)
	        {
				if (node.trie != null){
					try{
			            short indexRes;
			            if (Trie.USE_TRIE){
			            	indexRes = node.trie.trieFindVal(key);
			            }
			            else{
			            	indexRes = node.findIndex(key);
			            }
			            	
			            if (indexRes != -1)
			            {
			                return node.data[indexRes].value;
			            }
					}
					catch(NullPointerException ex){
						return null;
					}
				}
				else
				{
					return null;
				}
	        }
	    }
	    return null;
		
	}

	/*
	 * The method checks the count field and determines if
	 * the Node needs to be split after the removal of the pair. It than continues to copy the old values to the new Node\s.
	 */
	private void RemoveSetup(	LeapList l, long key,
						LeapNode n, LeapNode[] oldNode,
						boolean[] merge, boolean[] changed, int j) 
	{        
		 // Copy the old_node's properties to the new one.
        n.level = oldNode[0].level;    
        n.low   = oldNode[0].low;
        n.count = oldNode[0].count;
        n.live = false;

		// If a merge is required, update the new node's properties. Get the maximum level between both of the old nodes and increase the number of elements and maximum expected key in the new node.
        if(merge[j])
        {
            if (oldNode[1].level > n.level)
            {
                n.level = oldNode[1].level;
            }
            n.count += oldNode[1].count;
            n.high = oldNode[1].high;
        }
        else
        {
            n.high = oldNode[0].high;
        }

     // Call remove in order to copy all key-value pairs from the old node ( or 2 nodes if merge is required ) without the key to be removed.
        changed[j] = remove(oldNode, n, key, merge[j]);
	}
	
	/*
	 * The method copies the updated data field to the new nodes.
	 */
	private boolean remove(LeapNode[] old_node, LeapNode n,
				 long k, boolean merge) {
		int i,j;
		boolean changed = false;
		
		// copy all key-value pairs of old node to new one except the one associated with the given key 'k'
		for (i=0,j=0; j<old_node[0].count; j++)
	    {
			if(old_node[0].data[j].key != k){
				n.data[i].key = old_node[0].data[j].key;
	            n.data[i].value = old_node[0].data[j].value;
	            i++;
			}
			else
	        {
	            changed = true;
	            n.count--;
	        }
	    }
		
		// If node is merged, copy all keys from old_node[1] to node n as well.
		if(merge)
	    {
	        for (j=0; j<old_node[1].count; j++)
	        {
	            n.data[i].key = old_node[1].data[j].key;
	            n.data[i].value = old_node[1].data[j].value;
	            i++;
	        }
	    }
		n.trie=new Trie(n.data, n.count);
		
	    return changed;
	}

	/*
	 * The method calls the rangeQuery method for the list.
	 */
	public Object[] RangeQuery (LeapList l,long low, long high){
		return l.RangeQuery(low, high);
	}
	
}
