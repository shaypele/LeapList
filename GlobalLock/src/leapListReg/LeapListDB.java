package leapListReg;


import java.util.Random;

import utils.Trie;

import java.util.concurrent.locks.ReentrantLock;


public class LeapListDB {
	static final  int MAX_ROW = 10;
	public LeapList[] LeapLists = new LeapList[MAX_ROW];
	
	private final ReentrantLock dbLock = new ReentrantLock();

	
	public LeapListDB () {
		for (int i=0; i < MAX_ROW ; i++)
		{
			LeapLists[i] = new LeapList();
		}
	}
	
	
	public LeapList  GetListByIndex (int index){
		if (index < MAX_ROW){
			return LeapLists[index];
		}
		else{
			return null;
		}
	}
	

	public Object lookUp (LeapList l, long key){
		dbLock.lock();
		try {
			return l.lookUp(key);
		} finally {
			dbLock.unlock();
		}
	}
	
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
	
	byte getLevel(){
		Random rand = new Random();
		long r = rand.nextLong();
		byte l = 1;
		r = (r >> 4) & ((1 << (LeapList.MAX_LEVEL - 1)) -1);
		while ((r & 1)  > 0){
			l++;
			r >>= 1;
		}
		return l;
	}
	
	LeapNode updateSetup (LeapList l, long key, Object value, LeapNode[] pa, LeapNode[] na, 
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
				if (n.data[j].key == key){     //there is an int overwrite in the prof. cod. not sure what it does.
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
						
						// Count = i+1 . if we put the new key in the last place of the node (we know that it's the last place because of the split)
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

	private void RemoveReleaseAndUpdate(LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int k) {
		        if(changed[k])
		        {
		            // Update the next pointers of the new node
		            int i=0;
		            if (merge[k])
		            {   
		                for (; i < oldNode[1].level; i++)
		                    n.next[i] = oldNode[1].next[i];//.UnMark();
		            }
		            for (; i < oldNode[0].level; i++)
		                n.next[i] = oldNode[0].next[i];//.UnMark();
		            
		            
		            for(i = 0; i < n.level; i++)
		            {   
		                pa[i].next[i] = n;
		            }
		            n.live = true;
		            if(merge[k])
		            	oldNode[1].trie=null;

		            oldNode[0].trie=null;
		        }  
	} 
	
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

	LeapNode RemoveSetup(LeapList l, long key, LeapNode[] pa,
			LeapNode[] na, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int k) 
	{
			LeapNode n = new LeapNode();
			int total = 0;//might be problem
	        merge[k] = false;
	        l.searchPredecessor( key, pa, na);
	        oldNode[0] = na[0];
	        /* If the key is not present, just return */
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

	        if(merge[k])// this part of code is not in the paper.
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

	public Object[] RangeQuery (LeapList l,long low, long high){
		dbLock.lock();
		try {
			return l.RangeQuery(low, high);
		} finally {
			dbLock.unlock();
		}
	}
	
}
