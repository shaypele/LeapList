package leapListReg;
import java.util.concurrent.ThreadLocalRandom;
import utils.Trie;

import java.util.concurrent.locks.ReentrantLock;
/*
 * LeapListDB class represent the L LeapList data structure and it has an array of LeapLists.
 */

public class LeapListDB {
	static  int MAX_ROW;
	public LeapList[] LeapLists;
	//the global lock of the data structure.
	private final ReentrantLock dbLock = new ReentrantLock();
	
	public LeapListDB (int numberOfLists) {
		MAX_ROW = numberOfLists;
		LeapLists = new LeapList[MAX_ROW];
		for (int i=0; i < MAX_ROW ; i++)
		{
			//initiate all the LeapLists.
			LeapLists[i] = new LeapList();
		}
	}
	
	/*
	 * Returns the list for the matching index if the index is smaller then the number of lists
	 * else it returns null.
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
	 * It tries to hold the lock and when it succeed it calls the lookup method for the LeapList.
	 */
	public Object lookUp (LeapList l, long key){
		//hold the global lock
		dbLock.lock();
		try {
			//call the LeapList lookUp with the given key
			return l.lookUp(key);
		} finally {
			//always release the lock.
			dbLock.unlock();
		}
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It tries to hold the global lock.
	 * when it succeed it adds or updates the key[i] value[i] pair for LeapList[i], by calling updateSetup() and updateRelease().
	 */
	public void leapListUpdate (LeapList [] ll, long [] keys, Object [] values, int size){
		//try to hold the lock
		dbLock.lock();
		try {
			// pa, na - array, each pa and na is filled by search_predeccessors
			LeapNode[] pa = null ;
			LeapNode[] na = null ;
			LeapNode n ;
			// the newNode is an array of size two for the cases we need to split the node.
			LeapNode[] newNode = new LeapNode[2];
			//maxHeight[i] holds highest level of the nodes in list[i] in case the node split else it holds the level of the node.
			int [] maxHeight = new int[size];
			//newKeys is the actual keys we put in the lists after we adding them 2 to avoid the sentinel keys 0 and 1.
			long[] newKeys = new long[size];
			//if split[i] is true there is a split for the node in list[i].
			boolean [] split = new boolean [size];
			//if change[i] is true it means that we add/change the key value pair in list[i].
			boolean [] changed = new boolean [size];
			
			for(int i = 0; i < size; i++){
				
				newKeys[i] = keys[i] + 2 ; // avoid sentinel; 
			}
			//run over all the lists
			for (int i = 0; i < size ; i++){
				//create all the variables for the current list
				pa = new LeapNode[LeapList.MAX_LEVEL];
				na = new LeapNode[LeapList.MAX_LEVEL];
				newNode[0] = new LeapNode();
				newNode[1] = new LeapNode();
				n = updateSetup (ll[i], newKeys[i], values[i], pa, na, newNode, maxHeight, split, changed, i);
				updateRelease (pa, na, n, newNode, split, changed, i);
			}
		} finally {
			//always free the lock
			dbLock.unlock();
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
	 * The method calls the searchPredecessor method on the LeapList and key. It checks if the matching Node is
	 * at its maximum size and if so splits it. It then calls the insert method to copy the values to the updated node.
	 */
	private LeapNode updateSetup (LeapList l, long key, Object value, LeapNode[] pa, LeapNode[] na, 
									 LeapNode [] newNode, int maxHeight[], boolean [] split, boolean [] changed, int i){
		
			LeapNode n = null;
			// Get the predecessors (in all of the levels ) of the node that key should be added to into pa, get the successors of pa into na.
			// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node (or 2 new nodes is split is required), this node is returned
			//	into n.
			l.searchPredecessor(key, pa, na);	
			n = na[0];
			// If the node to be removed has reached its maximum size, it should be split into 2 new nodes.Otherwise only one node will replace it.
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
			// Copy values of the old node to the new node ( or 2 if there was a split ).
			changed[i] = insert(newNode, n, key, value, split[i]);
			return n;
	}
	
	/*
	 * The method fills in the values of the new Node\s and adds the key-value pair.
	 * it returns true if the key value pair was added/changed and false otherwise.
	 */
	boolean insert (LeapNode[] newNode, LeapNode n, long key, Object val, boolean split){
		boolean changed = false;
		int m = 0;
		int i = 0;
		int j = 0;
	
		// If there's a split, put NODE_SIZE/2 elements in one node and the rest in the other.
		// The 2 new nodes are now associated with the key range that was associated with one node 'n'
		if (split){
			newNode[0].low = n.low;
			newNode[0].count = (LeapList.NODE_SIZE/2);
			newNode[1].high = n.high;
			newNode[1].count = n.count - (LeapList.NODE_SIZE/2);
		}

		// Otherwise, copy old 'n' properties to the new node
		else
		{
			newNode[0].low = n.low;
			newNode[0].high = n.high;
			newNode[0].count = n.count;
		}
		// Add/update the given key-value pair (k,v),also  add all elements from node 'n' to the new set of nodes (divide between 2 node if there's a split )
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
					// Data is a sorted array therefore if next element has a bigger key then the given key 'k', that's is the place to put the given key-value pair into.
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
					
					//Copy elements from old node 'n' to the new node
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = n.data[j].value;
				}
				// Move to next node if split == 1 and new_node has reached it's assigned capacity 
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
				/* build the tries for the new nodes to allow quick access to the key's index in the data array.*/
				newNode[0].trie = new Trie(newNode[0].data, newNode[0].count);
				newNode[1].trie = new Trie(newNode[1].data, newNode[1].count);
			}
			else{
				// build a trie to allow quick access to the key's index in the data array.
				newNode[m].trie = new Trie(newNode[m].data, newNode[m].count);
			}
		}
		return changed;
	}
	
	/*
	 * For each list in l. Link the pointers of the new nodes to point to the successors in na. Also, re-link the pa pointers to point to the new nodes assigned.
	 * After that the new nodes are accessible, they are part of the lists.
	 */
	void updateRelease (LeapNode[] pa, LeapNode[] na, LeapNode n, LeapNode[] newNode, boolean[] split,
									boolean[] changed, int k){
			int i = 0;
			if (changed[k]){//if we add/change the node
	            // First point the next pointers of all level in the new node ( or 2 if there's a split)  to the assoicated successors in succs.
				if (split[k]){
					
					// If there is a split, distinguish between two case: one where new_node[1].level > new_node[0].level and the opposite.
					if (newNode[1].level > newNode[0].level){
						
					//  Since new_node[1].level = n.level and new_node[1].level > new_node[0].level, point all 'next' pointers in new_node[0] to new_node[1],
					//  Next, copy all 'next' pointers from n to new_node[1] (since they share the same maximum level)
						for (i = 0; i < newNode[0].level; i++){
							newNode[0].next[i] = newNode[1];
                            newNode[1].next[i] = n.next[i];
						}
						 for (; i < newNode[1].level; i++)
	                            newNode[1].next[i] = n.next[i];
					}
					else
                    {   
						// In this case only point part of new_node[0] 'next' pointers to new_node[1]. The others should point to na[i] , where i represents the higher levels.
						// This is because new_node[0] max level is bigger the original's node n's max level.
                        for (i = 0; i < newNode[1].level; i++)
                        {
                            newNode[0].next[i] = newNode[1];
                            newNode[1].next[i] = n.next[i];
                        }
                        for (; i < newNode[0].level; i++)
                            newNode[0].next[i] = na[i];
                    }
				}
				else
                {
					// If no split occurred, simply copy all 'next' pointers from n to new_node[0].
                    for (i = 0; i < newNode[0].level; i++)
                    {
                        newNode[0].next[i] = n.next[i];
                    }
                }
	            // Link the predecessors pa 'next' pointers of the node n to the new node ( or 2 if there's a split )
				for(i=0; i < newNode[0].level; i++)
                {
                    pa[i].next[i] = newNode[0];
                }
				// If there's a split and new_node[1].level > new_node[0].level , Link the predecessors pa 'next' pointers of the node n to new node[1]
                if (split[k] && (newNode[1].level > newNode[0].level)){
                    for(; i < newNode[1].level; i++)
                    { 	
                        pa[i].next[i] = newNode[1];
                    }
                }
             // Linking completed, mark the new nodes as live.
                newNode[0].live = true;
                if (split[k])
                    newNode[1].live = true;

			}
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It tries to get the lock.
	 * If successful it removes the key[i] value[i] pair for LeapList[i] (if exists), by calling removeSetup() and removeRelease().
	 */
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
		//try to hold the lock.
		dbLock.lock();
		try {
			// pa, na - - each pa and na is filled by search_predeccessors
		    LeapNode[]  pa = null;
		    LeapNode[] na = null;
		    LeapNode n = null;
		    //oldNode is the node that we want to remove from him the key value pair.
		    LeapNode[] oldNode = null;
			//newKeys is the actual keys we put in the lists after we adding them 2 to avoid the sentinel keys 0 and 1.
		    long[] newKeys = new long[size];
		    int j;
			//if change[i] is true it means that the key is in the list[i] and we remove the key value pair
			//if merge[i] is true there is a merge between the node and his successor in list[i].
		    boolean[] changed = new boolean[size], merge = new boolean[size];
	
		    for(j=0; j<size; j++)
		    {
		    	newKeys[j] =  keys[j]+2; // Avoid sentinel
		    }
		    
		  //run over all the lists
			for (int i = 0; i < size ; i++){
				//create all the variables for the current list
		    	pa = new LeapNode[LeapList.MAX_LEVEL];
				na = new LeapNode[LeapList.MAX_LEVEL];
				oldNode = new LeapNode[2];
		    	n = RemoveSetup(ll[i],newKeys[i], pa, na, oldNode, merge, changed, i);
		    	RemoveReleaseAndUpdate(pa, na, n, oldNode, merge, changed, i);
		    }
		} finally {
			//always free the lock
			dbLock.unlock();
		}
	}

	/*
	 * Link the 'next' pointers of the node n to point to the successors of the old node. Also, link the predecessors pa pointers to point to the node n .
	 * After that the new nodes are not accessible, and they are not part of the lists.
	 */
	private void RemoveReleaseAndUpdate(LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int k) {
		
		// update links only if the removal was actually needed
		        if(changed[k])
		        {
		            int i=0;
		            if (merge[k])
		            {   
		            	// If a merge is required, get the 'next' pointers from oldNode[1] into the new node 'n'. Later on if oldNode[0] has a higher max level than oldNode[1], node 'n' will get the 'next' pointers from it.
		    			// If no merge is required, copy all next pointers from oldNode[0] into the new node 'n'.  
		                for (; i < oldNode[1].level; i++)
		                    n.next[i] = oldNode[1].next[i];
		            }
		            for (; i < oldNode[0].level; i++)
		                n.next[i] = oldNode[0].next[i];
		            
		            // n.level is the max level between oldNode[0] and oldNode[1] ( in case of a merge ), so the merge case is covered here as well.
		            // Link the predecessors pa 'next' pointers of oldNode[0] ( in case there's a merge and oldNode[1].level > oldNode[0].level oldNode[1]'s pa will point to n as well )
		            //to the new node n. 
		            for(i = 0; i < n.level; i++)
		            {   
		                pa[i].next[i] = n;
		            }
					// Node n is fully linked to the list, set it as live.
		            n.live = true;
		            if(merge[k])
		            	oldNode[1].trie=null;

		            oldNode[0].trie=null;
		        }  
	} 

	/*
	 * The method searches for a key in a given Node and return the matching object if found. Return null if not.
	 */
	Object find(LeapNode node,long key){
		
		if(node!=null){
			if (node.count > 0)
	        {
				//call the trieFindVal function from the node's trie
	            short indexRes = node.trie.trieFindVal(key);
	            if (indexRes != -1)
	            {
	                return node.data[indexRes].value;
	            }
	        }
	    }
	    return null;
		
	}

	/*
	 * The method calls the searchPredecessor method on the LeapList and key. It checks if the matching node
	 * contains the desired key. If not it returns a new Node. If the key is found it checks the count field and determines if
	 * the Node needs to be split after the removal of the pair. It than continues to copy the old values to the new Node\s.
	 */
	LeapNode RemoveSetup(LeapList l, long key, LeapNode[] pa,
			LeapNode[] na, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int i) 
	{
			LeapNode n = new LeapNode();
			int total = 0;
	        merge[i] = false;
	        // Get the predecessors (in all of the levels ) of the node that key should be added to into pa, get the successors of pa into na.
			// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node, this node is returned
			//	into old_node.
	        l.searchPredecessor( key, pa, na);
	        oldNode[0] = na[0];
	        
	        /* If the key is not present set changed[i] = false and return n  */
	        if (find(oldNode[0], key) == null)
	        {
	            changed[i] = false;
	            	return n;
	        }
	        total = oldNode[0].count;
			// Get the next node of old_node[0] in order to decide later on if merge is needed or not.
	        oldNode[1] = oldNode[0].next[0];
	       
	        if(oldNode[1] != null)
	        {
	            total = total + oldNode[1].count;
				// if(total -1 <= NODE_SIZE) a merge is required, so the two nodes will become one new node.
	            if(total - 1<= LeapList.NODE_SIZE)
	            {
	                merge[i] = true; 
	            }
	        }
	        
	     // Copy the old_node's properties to the new one.
	        n.level = oldNode[0].level;    
	        n.low   = oldNode[0].low;
	        n.count = oldNode[0].count;
	        n.live = false;

			// If a merge is required, update the new node's properties. Get the maximum level between both of the old nodes and increase the number of elements and maximum expected key in the new node.
	        if(merge[i])
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
	        changed [i] = remove(oldNode, n, key, merge[i]);
	        return n;
	}
	
	/*
	 * Gets an array of old_node (contains 2 in case there's a merge) , node n to set the new key-value pairs to, key 'k' to remove, 'merge' - 1 to merge/0 not to merge,
	 * Return true if given key was found and removed, false otherwise.
	 */
	private boolean remove(LeapNode[] old_node, LeapNode n,
				 long k, boolean merge) {
		int i,j;
		boolean changed = false;
		
		// copy all key-value pairs of old node to new one except the one associated with the given key 
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
		// build a trie to allow quick access to the key's index in the data array.
		n.trie=new Trie(n.data, n.count);
		
	    return changed;
	}

	/*
	 * The method calls the rangeQuery method for the list.
	 */
	public Object[] RangeQuery (LeapList l,long low, long high){
		//try to hold the global lock
		dbLock.lock();
		try {
			// call the LeapList RangeQuery function with the give low and high
			return l.RangeQuery(low, high);
		} finally {
			//always free the lock
			dbLock.unlock();
		}
	}
	
}
