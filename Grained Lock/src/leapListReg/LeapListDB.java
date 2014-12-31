package leapListReg;

import java.util.concurrent.ThreadLocalRandom;
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
						updateSetup (ll[i], keys[i], values[i], n[i], newNode[i], maxHeight, split, changed, i);
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
						updateRelease (pa[i], na[i], n[i], newNode[i], split, changed, i);
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
				}
			}
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
	 * The method checks if the matching Node is
	 * at its maximum size and if so splits it. It then calls the insert method to copy the values to the updated node.
	 */
	private void updateSetup (  LeapList l, long key, Object value,  
										LeapNode n, LeapNode [] newNode, int [] maxHeight, boolean[] split, boolean[] changed, int i){
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
			changed [i] = insert(newNode, n, key, value, split[i]);
	}
	
	/*
	 * The method fills in the values of the new Node\s and adds the key-value pair.
	 */
	private boolean insert (LeapNode[] newNode, LeapNode n, long key, Object val, boolean split){
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
	
	/*
	 * The method inserts the new Node\s into the list by updating the relevant pointers using the predecessor and successor arrays.
	 */
	private void  updateRelease (LeapNode[] pa, LeapNode[] na, LeapNode n, LeapNode[] newNode, boolean[] split,
									boolean[] changed, int j){
			int i = 0;
			
			if (changed[j]){
				if (split[j]){
					if (newNode[1].level > newNode[0].level){
						for (i = 0; i < newNode[0].level; i++){
							newNode[0].setNext(i, newNode[1]) ;
                            newNode[1].setNext(i, n.getNext(i));
						}
						 for (; i < newNode[1].level; i++)
	                            newNode[1].setNext(i,n.getNext(i));
					}
					else
                    {   
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
                    for (i = 0; i < newNode[0].level; i++)
                    {
                        newNode[0].setNext(i, n.getNext(i));
                    }
                }
				
				for(i=0; i < newNode[0].level; i++)
                {
                    pa[i].setNext(i, newNode[0]);
                }
                if (split[j] && (newNode[1].level > newNode[0].level)){
                    for(; i < newNode[1].level; i++)
                    { 	
                        pa[i].setNext(i, newNode[1]);
                    }
                }
                
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
			     
			     /* If the key is not present, just return 
			      * In case, a node was previously locked, unlock it*/
		         if (find(oldNode[j][0], keys[j]) == null)
		         {
		        	 if (!oldNode[j][0].live){
		        		 continue;
		        	 }
		        		 
		        	 changed[j] = false;
		        	 
		        	 oldNode[j][1] = oldNode[j][0].getNext(0);
		 			if (oldNode[j][0].Marked && isMarkedArr[0]){
	 					oldNode[j][0].unlock();
		 				oldNode[j][0].Marked = false;
		 				isMarkedArr[0] = false;
		 			}
		 				
	 				if (oldNode[j][1]!=null && oldNode[j][1].Marked && isMarkedArr[1]){
	 					oldNode[j][1].unlock();
		 				oldNode[j][1].Marked = false;
		 				isMarkedArr[1] = false;
	 				}
		            break;
		         }
		        
		        // Find out if it's a merge or not.
		         // Merge if it's there are less elements two nodes than the merge threshold.
		         // Merge & split thresholds should be different to avoid constant merge/split 
		        oldNode[j][1] = oldNode[j][0].getNext(0);
		 		if (oldNode[j][1]!= null && 
		          		(oldNode[j][0].count + oldNode[j][1].count - 1) <= LeapList.NODE_SIZE - 10 ) 
		 		{
		          		merge[j] = true;
		 		}
		 		else
		 		{
		 			int count1 = 0;
		 			if (oldNode[j][1] != null )
		 				count1 = oldNode[j][1].count;
		 			merge[j] = false;
		 		}
		         
			     try{
			    	 LeapNode pred,succ, prevPred =null;
			    	 boolean valid = true;
			    	 if ( 	isMarkedArr[0] || 
			    			(  !oldNode[j][0].Marked &&  oldNode[j][0].live ) ){
			    		 
							if (merge[j] && !(isMarkedArr[1] || 
													(  !oldNode[j][1].Marked &&  oldNode[j][1].live ))  )
							{
								continue;
							}
			    		if ( !merge[j] )
			    		{
				    		if (!isMarkedArr[0])
				    		{
				    			 oldNode[j][0].lock();
				    			 if (oldNode[j][0].Marked){
				    				 oldNode[j][0].unlock();
				    				 continue;
				    			 }
				    			 oldNode[j][0].Marked = true;
				    			 isMarkedArr[0] = true;
				    		}
			    		}
			    		else
			    		{
			    			if (!isMarkedArr[0])
				    		{
			    				// Lock old node 1
			    				if (!isMarkedArr[1])
					    		{
					    			 oldNode[j][1].lock();
					    			 if (oldNode[j][1].Marked){
					    				 oldNode[j][1].unlock();
					    				 continue;
					    			 }
					    			 oldNode[j][1].Marked = true;
					    			 isMarkedArr[1] = true;
					    		}
			    				
			    				// Lock old node 0
			    				 oldNode[j][0].lock();
				    			 if (oldNode[j][0].Marked){
				    				 oldNode[j][0].unlock();
				    				 continue;
				    			 }
				    			 oldNode[j][0].Marked = true;
				    			 isMarkedArr[0] = true;
				    		}
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
			         	
			         	RemoveSetup(ll[j],keys[j], n[j], oldNode[j], merge, changed,j);
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
			         	
			         	RemoveReleaseAndUpdate(pa[j], na[j], n[j], oldNode[j], merge, changed, j, pa_Node1[j], na_Node1[j]);
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

	/*
	 * The method removes the Node from the list by updating the relevant pointers using the predecessor and successor arrays.
	 */
	private void RemoveReleaseAndUpdate(LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int j,LeapNode[] pa_Node1,LeapNode[] na_Node1) {
		 
	        if(changed[j])
	        {
	        	
	        	  if(merge[j])
	            {
	            	oldNode[1].live = false;
	            	oldNode[1].trie=null;
	            }

	            oldNode[0].live = false;
	            oldNode[0].trie=null;
	        	
	            // Update the next pointers of the new node
	            int i=0;
	            if (merge[j])
	            {   
	                for (; i < oldNode[1].level; i++)
	                    n.setNext(i, oldNode[1].getNext(i));//.UnMark();
	            }
	            for (; i < oldNode[0].level; i++)
	                n.setNext(i, oldNode[0].getNext(i));//.UnMark();
	            
	            
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

	/*
	 * The method checks the count field and determines if
	 * the Node needs to be split after the removal of the pair. It than continues to copy the old values to the new Node\s.
	 */
	private void RemoveSetup(	LeapList l, long key,
						LeapNode n, LeapNode[] oldNode,
						boolean[] merge, boolean[] changed, int j) 
	{        
        n.level = oldNode[0].level;    
        n.low   = oldNode[0].low;
        n.count = oldNode[0].count;
        n.live = false;

        if(merge[j])// this part of code is not in the paper.
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

        changed[j] = remove(oldNode, n, key, merge[j]);
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
		return l.RangeQuery(low, high);
	}
	
}
