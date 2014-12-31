package leapListReg;


import java.util.concurrent.ThreadLocalRandom;
import utils.Trie;

import java.util.concurrent.locks.ReentrantLock;


public class LeapListDB {
	static final  int MAX_ROW;
	public LeapList[] LeapLists;
	
	private final ReentrantLock dbLock = new ReentrantLock();

	
	public LeapListDB (int numberOfLists) {
		MAX_ROW = numberOfLists;
		leapLists = new LeapLists[MAX_ROW];
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
	 * It tries to get the lock and if successful calls the lookup method for the LeapList.
	 */
	public Object lookUp (LeapList l, long key){
		dbLock.lock();
		try {
			return l.lookUp(key);
		} finally {
			dbLock.unlock();
		}
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It tries to get the lock.
	 * If successful it adds or updates the key[i] value[i] pair for LeapList[i], by calling updateSetup() and updateRelease().
	 */
	public void leapListUpdate (LeapList [] ll, long [] keys, Object [] values, int size){
		dbLock.lock();
		try {
			LeapNode[][] pa = new LeapNode[size][LeapList.MAX_LEVEL];
			LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
			LeapNode[] n = new LeapNode[size];
			LeapNode [][] newNode = new LeapNode[size][2];
			int [] maxHeight = new int[size];
			boolean [] split = new boolean [size];
			boolean [] changed = new boolean [size];
			
			for(int i = 0; i < size; i++){
				newNode[i][0] = new LeapNode();
				newNode[i][1] = new LeapNode();
				keys[i] += 2 ; // avoid sentinel; 
			}
			for (int i = 0; i < size ; i++){
				n[i] = updateSetup (ll[i], keys[i], values[i], pa[i], na[i], newNode[i], maxHeight, split, changed, i);
				updateRelease (pa[i], na[i], n[i], newNode[i], split, changed, i);
			}
		} finally {
			dbLock.unlock();
		}
		
	}
	
	/*
	 * The method uses Random() to return a byte value with a 50% chance increment starting from 1,
	 */
	private byte getLevel(){
		//Random rand = new Random();
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
			l.searchPredecessor(key, pa, na);	
			n = na[0];
			if (n.count == LeapList.NODE_SIZE){
				split[i] = true;
				newNode[1].level = n.level;
				newNode[0].level = getLevel();
				maxHeight[i] = (newNode[0].level > newNode[1].level) ? newNode[0].level: newNode[1].level;
			}
			else{
				split[i] = false;
				newNode[0].level = n.level;
				maxHeight[i] = newNode[0].level;
			}
			changed[i] = insert(newNode, n, key, value, split[i]);
			return n;
	}
	
	/*
	 * The method fills in the values of the new Node\s and adds the key-value pair.
	 */
	boolean insert (LeapNode[] newNode, LeapNode n, long key, Object val, boolean split){
		boolean changed = false;
		int m = 0;
		int i = 0;
		int j = 0;
	
		
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
		if (n.count == 0){
			newNode[m].data[0].key = key;
			newNode[m].data[0].value = val;
			newNode[m].count = 1;
			changed = true;
			newNode[m].trie = new Trie(key, (short)0);
		}
		else{
			for (j = 0; j < n.count; i++, j++){
				if (n.data[j].key == key){   
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = val;
					changed = true;
				}
				else
				{
					
					if ((!changed) && (n.data[j].key > key)){
						newNode[m].data[i].key = key;
						newNode[m].data[i].value = val;
						newNode[m].count++;
						changed = true;
						
						if ((m != 1) && split && (newNode[0].count == (i+1))){
							newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
							i = -1;
							m = m + 1;
						}
						
						i++;
					}
					
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = n.data[j].value;
				}
				if ((m != 1) && split && (newNode[0].count == (i+1))){
					newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
					i = -1;
					m = m+1;
				}
			}
			
			if (!changed){
				newNode[m].count++;
				newNode[m].data[i].key = key;
				newNode[m].data[i].value = val;
				changed = true;
			}
			
			if (split){
				newNode[0].trie = new Trie(newNode[0].data, newNode[0].count);
				newNode[1].trie = new Trie(newNode[1].data, newNode[1].count);
			}
			else{
				newNode[m].trie = new Trie(newNode[m].data, newNode[m].count);
			}
		}
		return changed;
	}
	
	/*
	 * The method inserts the new Node\s into the list by updating the relevant pointers using the predecessor and successor arrays.
	 */
	void updateRelease (LeapNode[] pa, LeapNode[] na, LeapNode n, LeapNode[] newNode, boolean[] split,
									boolean[] changed, int k){
			int i = 0;
			if (changed[k]){
				if (split[k]){
					if (newNode[1].level > newNode[0].level){
						for (i = 0; i < newNode[0].level; i++){
							newNode[0].next[i] = newNode[1];
                            newNode[1].next[i] = n.next[i];
						}
						 for (; i < newNode[1].level; i++)
	                            newNode[1].next[i] = n.next[i];
					}
					else
                    {   
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
                    for (i = 0; i < newNode[0].level; i++)
                    {
                        newNode[0].next[i] = n.next[i];
                    }
                }
				
				for(i=0; i < newNode[0].level; i++)
                {
                    pa[i].next[i] = newNode[0];
                }
                if (split[k] && (newNode[1].level > newNode[0].level)){
                    for(; i < newNode[1].level; i++)
                    { 	
                        pa[i].next[i] = newNode[1];
                    }
                }

			}
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It tries to get the lock.
	 * If successful it removes the key[i] value[i] pair for LeapList[i] (if exists), by calling removeSetup() and removeRelease().
	 */
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
		dbLock.lock();
		try {
		    LeapNode[][]  pa = new LeapNode[size][LeapList.MAX_LEVEL];
		    LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
		    LeapNode[] n = new LeapNode[size];
		    LeapNode[][] oldNode = new LeapNode[size][2];
		    int j;
		    boolean[] changed = new boolean[size], merge = new boolean[size];
		   
	
		    for(j=0; j<size; j++)
		    {
		        keys[j]+=2; // Avoid sentinel
		    }
		    for (int i = 0; i < size; i++){
		    	n[i] = RemoveSetup(ll[i],keys[i], pa[i], na[i], oldNode[i], merge, changed, i);
		    	RemoveReleaseAndUpdate(pa[i], na[i], n[i], oldNode[i], merge, changed, i);
		    }
		} finally {
			dbLock.unlock();
		}
	}

	/*
	 * The method removes the Node from the list by updating the relevant pointers using the predecessor and successor arrays.
	 */
	private void RemoveReleaseAndUpdate(LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int k) {
		        if(changed[k])
		        {
		            int i=0;
		            if (merge[k])
		            {   
		                for (; i < oldNode[1].level; i++)
		                    n.next[i] = oldNode[1].next[i];
		            }
		            for (; i < oldNode[0].level; i++)
		                n.next[i] = oldNode[0].next[i];
		            
		            
		            for(i = 0; i < n.level; i++)
		            {   
		                pa[i].next[i] = n;
		            }
		            n.live = true;
		           // if(merge[k])
		            //	oldNode[1].trie=null;

		            //oldNode[0].trie=null;
		        }  
	} 

	/*
	 * The method searches for a key in a given Node and return the matching object if found. Return null if not.
	 */
	Object find(LeapNode node,long key){
		
		if(node!=null){
			if (node.count > 0)
	        {
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
			boolean[] merge, boolean[] changed, int k) 
	{
			LeapNode n = new LeapNode();
			int total = 0;
	        merge[k] = false;
	        l.searchPredecessor( key, pa, na);
	        oldNode[0] = na[0];
	
	        if (find(oldNode[0], key) == null)
	        {
	            changed[k] = false;
	            	return n;
	        }
	        total = oldNode[0].count;
	        oldNode[1] = oldNode[0].next[0];
	       
	        if(oldNode[1] != null)
	        {
	            total = total + oldNode[1].count;
	            if(total - 1<= LeapList.NODE_SIZE)
	            {
	                merge[k] = true; 
	            }
	        }
	        n.level = oldNode[0].level;    
	        n.low   = oldNode[0].low;
	        n.count = oldNode[0].count;
	        n.live = false;

	        if(merge[k])
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

	        changed [k] = remove(oldNode, n, key, merge[k]);
	        return n;
	}
	
	/*
	 * The method copies the updated data field to the new nodes.
	 */
	private boolean remove(LeapNode[] old_node, LeapNode n,
				 long k, boolean merge) {
		int i,j;
		boolean changed = false;
		
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
		dbLock.lock();
		try {
			return l.RangeQuery(low, high);
		} finally {
			dbLock.unlock();
		}
	}
	
}
