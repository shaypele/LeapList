package leapListReg;


import java.util.Random;

import utils.Trie;

public class LeapListDB {
	static final  int MAX_ROW = 10;
	public volatile LeapList[] LeapLists = new LeapList[MAX_ROW];
	
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
		return l.lookUp(key);
	}
	
	public void leapListUpdate (LeapList [] ll, long [] keys, Object [] values, int size){
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
		
		for (int i = 0 ; i < size; i++){
			LeapNode lastLockedNode = null;
			boolean isMarked = false;
			while (true){
				// Get predecessors here, but create new nodes only after lock is acquired
				ll[i].searchPredecessor(keys [i], pa[i], na[i]);	
				n[i] = na[i][0];
				// TODO: is needed.? it was put here to prevent a scneraio where n[i] is change by a different predecessor..
				if (lastLockedNode != null && lastLockedNode != n[i]){
					if (lastLockedNode.nodeLock.isLocked()){
						lastLockedNode.unlock();
					}
					lastLockedNode.Marked = false;
					isMarked = false;
				}
				byte highestLocked = -1;
				try{
					LeapNode pred,succ, prevPred =null;
					boolean valid = true;
					if (isMarked ||
						(!n[i].Marked && n[i].live ) )
					{
						if ( !isMarked ){
							n[i].lock();
							if (n[i].Marked){
								n[i].unlock();
								continue;
							}
							lastLockedNode = n[i];
							n[i].Marked = true;
							isMarked = true;
						}
						// Create new nodes and decide whether to split or not only after lock has been acquired
						updateSetup (ll, keys, values, size, n, newNode, maxHeight, split, changed, i);
						for ( int level = 0; valid && ( level < maxHeight[i] ); level++){
							pred = pa[i][level];
							succ = na[i][level];
							if (pred != prevPred){
								pred.lock();
								highestLocked = (byte) level;
								prevPred = pred;
							}
							valid = !pred.Marked && pred.getNext(level) == succ /*&& (n[i].getNext(level) == null || (n[i].getNext(level) != null && n[i].getNext(level).live)  )*/;
						}
						if (!valid){
							continue;
						}	
						updateRelease (size, pa, na, n, newNode, split, changed, i);
						n[i].unlock();
						break;
					}
					else
					{
						continue;
					}
				}	
				finally{
					LeapNode prevPred = null;
					for (int j = 0 ; j <= highestLocked ; j++ ){
						if ( pa[i][j]!=prevPred )
						{
							pa[i][j].unlock();
							prevPred = pa[i][j];
						}
					}
					
					/*if ( isMarked ){
						//n[i].lock();
						if (n[i].Marked && n[i].nodeLock.isHeldByCurrentThread() ){
							n[i].unlock();
							lastLockedNode = null;
							n[i].Marked = false;
							isMarked = false;
						}
					}*/
				}
			}
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
	
	 void updateSetup (  LeapList[] ll, long [] keys, Object [] values, int size, 
										LeapNode[] n, LeapNode [][] newNode, int [] maxHeight, boolean[] split, boolean[] changed, int i){
			if (n[i].count == LeapList.NODE_SIZE){
				split[i] = true;
				newNode[i][1].level = n[i].level;
				newNode[i][0].level = getLevel();
				maxHeight[i] = (newNode[i][0].level > newNode[i][1].level) ? newNode[i][0].level: newNode[i][1].level;
			}
			else{
				split[i] = false;
				newNode[i][0].level = n[i].level;
				maxHeight[i] = newNode[i][0].level;
			}
			changed [i] = insert(newNode[i], n[i], keys[i], values[i], split[i]);
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
	
	
	void  updateRelease (int size, LeapNode[][] pa, LeapNode[][] na, LeapNode[] n, LeapNode[][] newNode, boolean[] split,
									boolean[] changed, int j){
			int i = 0;
			
			if (changed[j]){
				if (split[j]){
					if (newNode[j][1].level > newNode[j][0].level){
						for (i = 0; i < newNode[j][0].level; i++){
							newNode[j][0].setNext(i, newNode[j][1]) ;
                            newNode[j][1].setNext(i, n[j].getNext(i));
						}
						 for (; i < newNode[j][1].level; i++)
	                            newNode[j][1].setNext(i,n[j].getNext(i));
					}
					else
                    {   
                        for (i = 0; i < newNode[j][1].level; i++)
                        {
                            newNode[j][0].setNext(i, newNode[j][1]);
                            newNode[j][1].setNext(i, n[j].getNext(i));
                        }
                        for (; i < newNode[j][0].level; i++){
                            newNode[j][0].setNext(i, na[j][i]);
                        }
                    }
				}
				else
                {
                    for (i = 0; i < newNode[j][0].level; i++)
                    {
                        newNode[j][0].setNext(i, n[j].getNext(i));
                    }
                }
				
				for(i=0; i < newNode[j][0].level; i++)
                {
                    pa[j][i].setNext(i, newNode[j][0]);
                }
                if (split[j] && (newNode[j][1].level > newNode[j][0].level)){
                    for(; i < newNode[j][1].level; i++)
                    { 	
                        pa[j][i].setNext(i, newNode[j][1]);
                    }
                }
                
                newNode[j][0].live = true;
                if (split[j]){
                	newNode[j][1].live = true;
                }
                n[j].live = false;
            	
            	
            	/*if (n[j].next[0] == null){
            		int p = 5;
            		p = 9 * 9;
            	}
            	*/
            	
            	
			}
		
	}
	
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
	
	    LeapNode[][]  pa = new LeapNode[size][LeapList.MAX_LEVEL];
	    LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
	    LeapNode[][] pa_Node1 = new LeapNode[size][LeapList.MAX_LEVEL];
 		LeapNode[][] na_Node1 = new LeapNode[size][LeapList.MAX_LEVEL];
	    LeapNode[] n = new LeapNode[size];
	    LeapNode[][] oldNode = new LeapNode[size][2];
	    int j;
	    boolean[] changed = new boolean[size], merge = new boolean[size];
	   

	    for(j=0; j<size; j++)
	    {
	        n[j] = new LeapNode();
	        keys[j]+=2; // Avoid sentinel
	        
	        boolean[] isMarkedArr = new boolean[2]; 
	        isMarkedArr[0] = false;
	        isMarkedArr[1] = false;
	        
	        while (true){
	        	// Get predecessors here, but create new nodes only after lock is acquired
				 ll[j].searchPredecessor( keys[j], pa[j], na[j]);
			     oldNode[j][0] = na[j][0];
			     byte highestLocked = -1;
			     
			     /* If the key is not present, just return */

		         if (find(oldNode[j][0], keys[j]) == null)
		         {
		        	 if (!oldNode[j][0].live){
		        		 continue;
		        	 }
		        		 
		        	 changed[j] = false;
		             break;
		         }
		         
			     try{
			    	 LeapNode pred,succ, prevPred =null;
			    	 boolean valid = true;
			    	 if ( 	isMarkedArr[0] || 
			    			(  !oldNode[j][0].Marked &&  oldNode[j][0].live ) ){
			    		 
			    		 if (!isMarkedArr[0]){
			    			 oldNode[j][0].lock();
			    			 if (oldNode[j][0].Marked){
			    				 oldNode[j][0].unlock();
			    				 continue;
			    			 }
			    			 oldNode[j][0].Marked = true;
			    			 isMarkedArr[0] = true;
			    		}
			    		 
			    		oldNode[j][1] = oldNode[j][0].getNext(0);
			         	if (oldNode[j][1]!= null && 
			         		(oldNode[j][0].count + oldNode[j][1].count - 1) <= LeapList.NODE_SIZE ) 
			         	{
			         		merge[j] = true;
			         	}
			         	else
			         	{
			         		merge[j] = false;
			         	}
			         	
			         	// Mark and lock second node
			         	if (merge[j] && !isMarkedArr[1]){
			         		 if (!oldNode[j][1].tryLock()){
			         			oldNode[j][0].Marked = false;
				    			 isMarkedArr[0] = false;
			         			oldNode[j][0].unlock();
			         			 continue;
			         		 }
			    			 if (oldNode[j][1].Marked){
			    				 oldNode[j][1].unlock();
			    				 continue;
			    			 }
			    			 oldNode[j][1].Marked = true;
			    			 isMarkedArr[1] = true;
			         	}
			         	
			         	RemoveSetup(ll,keys, size, n, oldNode, merge, changed,j);
			         	//first, lock all prevs of node 0. Later of, if needed, lock preds of node 1.
			         	byte level;
			         	for ( level = 0; valid && ( level < oldNode[j][0].level ); level++){
							pred = pa[j][level];
							succ = na[j][level];
							if (pred != prevPred){
								pred.lock();
								highestLocked = level;
								prevPred = pred;
							}
							valid = !pred.Marked && pred.getNext(level) == succ;
						}
						if (!valid){
							continue;
						}	
						
						// if node 1's level is bigger than node 0's level, lock preds of higher level of node 1. 
			         	if ( merge[j] && ( oldNode[j][0].level < oldNode[j][1].level ) ){
			         		// Find preds of node 1.
			         		ll[j].searchPredecessor(oldNode[j][1].high, pa_Node1[j], na_Node1[j] );
			         		for(; valid && ( level < oldNode[j][1].level ); level++){
			         			pred = pa_Node1[j][level];
								succ = na_Node1[j][level];
								if (pred != prevPred){
									pred.lock();
									highestLocked = level;
									prevPred = pred;
								}
								valid = !pred.Marked && pred.getNext(level) == succ;
							}
							if (!valid){
								continue;
							}	
			         	}
			         	
			         	RemoveReleaseAndUpdate(size,pa,na,n,oldNode,merge,changed,j,pa_Node1,na_Node1);
			         	oldNode[j][0].unlock();
			         	if (merge[j]){
			         		oldNode[j][1].unlock();
			         	}
			         	break;
			    	 }
			    	 else
			    	 {
			    		 continue;
			    	 }
			     }
			     finally{
			    	 LeapNode prevPred = null;
			    	 int iterateTill = -1;
			    	 byte level;
			    	 
			    	 if ( highestLocked < oldNode[j][0].level ){
			    		 iterateTill = highestLocked; 
			    	 }
			    	 else{
			    		 iterateTill = oldNode[j][0].level - 1;
			    	 }
			    	 // First unlock all pred of node 0.
					 for (level = 0 ; level <= iterateTill ; level++ ){
						if ( pa[j][level]!=prevPred )
						{
							pa[j][level].unlock();
							prevPred = pa[j][level];
						}
					 }
					 
					 // if needed , unlock rest of preds of node 1.
					if ( highestLocked >= oldNode[j][0].level ){
						iterateTill = highestLocked; 
						for (; level <= iterateTill ; level++ ){
							if ( pa_Node1[j][level]!=prevPred )
							{
								pa_Node1[j][level].unlock();
								prevPred = pa_Node1[j][level];
							}
						}
					}
			     }
	        }
	        
		    
		  
		    
	    }
	    
	}

	

	private void RemoveReleaseAndUpdate(int size, LeapNode[][] pa,
			LeapNode[][] na, LeapNode[] n, LeapNode[][] oldNode,
			boolean[] merge, boolean[] changed, int j,LeapNode[][] pa_Node1,LeapNode[][] na_Node1) {
		 
	        if(changed[j])
	        {
	            // Update the next pointers of the new node
	            int i=0;
	            if (merge[j])
	            {   
	                for (; i < oldNode[j][1].level; i++)
	                    n[j].setNext(i, oldNode[j][1].getNext(i));//.UnMark();
	            }
	            for (; i < oldNode[j][0].level; i++)
	                n[j].setNext(i, oldNode[j][0].getNext(i));//.UnMark();
	            
	            
	            for(i = 0; i < oldNode[j][0].level; i++)
	            {   
	                pa[j][i].setNext(i, n[j]);
	            }
	            
	            if ( merge[j] && ( oldNode[j][0].level < oldNode[j][1].level ) ){
	            	for(; i < oldNode[j][1].level; i++)
		            {  
	            		pa_Node1[j][i].setNext(i, n[j]);
		            }
	            }
	            
	            n[j].live = true;
	            if(merge[j])
	            {
	            	oldNode[j][1].live = false;
	            	oldNode[j][1].trie=null;
	            }

	            oldNode[j][0].live = false;
	            oldNode[j][0].trie=null;
	        }
	        else
	        {
	            n[j].trie=null;
	        } 
	}
	
	Object find(LeapNode node,long key){
		
		if(node!=null && node.live){
			if (node.count > 0)
	        {
				if (node.trie != null){
					try{
			            short indexRes = node.trie.trieFindVal(key);
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

	void RemoveSetup(	LeapList[] ll, long[] keys,int size,
						LeapNode[] n, LeapNode[][] oldNode,
						boolean[] merge, boolean[] changed, int j) 
	{        
        n[j].level = oldNode[j][0].level;    
        n[j].low   = oldNode[j][0].low;
        n[j].count = oldNode[j][0].count;
        n[j].live = false;

        if(merge[j])// this part of code is not in the paper.
        {
            if (oldNode[j][1].level > n[j].level)
            {
                n[j].level = oldNode[j][1].level;
            }
            n[j].count += oldNode[j][1].count;
            n[j].high = oldNode[j][1].high;
        }
        else
        {
            n[j].high = oldNode[j][0].high;
        }

        changed[j] = remove(oldNode[j], n[j], keys[j], merge[j]);
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
		return l.RangeQuery(low, high);
	}
	
}
