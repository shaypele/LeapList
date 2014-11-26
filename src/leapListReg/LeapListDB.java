package leapListReg;


import java.util.Random;

import utils.Trie;

public class LeapListDB {
	static final  int MAX_ROW = 4;
	LeapList[] LeapLists = new LeapList[MAX_ROW];
	
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
		}
		
		updateSetup (ll, keys, values, size, pa, na, n, newNode, maxHeight, split, changed);
		updateLT (size, pa, na, n, newNode, maxHeight, changed);
		updateRelease (size, pa, na, n, newNode, split, changed);
		
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
	
	 void updateSetup (LeapList[] ll, long [] keys, Object [] values, int size, LeapNode[][] pa, LeapNode[][] na, 
										LeapNode[] n, LeapNode [][] newNode, int [] maxHeight, boolean[] split, boolean[] changed){
		
		for (int i = 0; i < size; i ++){
			ll[i].searchPredecessor(keys [i], pa[i], na[i]);	
			n[i] = na[i][0];
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
					newNode[m].data[i].value = val;
					changed = true;
				}
				
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
	
	//runs test for transactional memory multi thread
	void updateLT (int size, LeapNode [][] pa, LeapNode [][] na, LeapNode[] n, LeapNode[][] newNode, int[] maxHeight,
								boolean[] changed){
	}
	
	void updateRelease (int size, LeapNode[][] pa, LeapNode[][] na, LeapNode[] n, LeapNode[][] newNode, boolean[] split,
									boolean[] changed){
		for (int j = 0; j < size; j++){
			int i = 0;
			
			if (changed[j]){
				if (split[j]){
					if (newNode[j][1].level > newNode[j][0].level){
						for (i = 0; i < newNode[j][0].level; i++){
							newNode[j][0].next[i] = newNode[j][1];
                            newNode[j][1].next[i] = n[j].next[i];
						}
						 for (; i < newNode[j][1].level; i++)
	                            newNode[j][1].next[i] = n[j].next[i];
					}
					else
                    {   
                        for (i = 0; i < newNode[j][1].level; i++)
                        {
                            newNode[j][0].next[i] = newNode[j][1];
                            newNode[j][1].next[i] = n[j].next[i];
                        }
                        for (; i < newNode[j][0].level; i++)
                            newNode[j][0].next[i] = na[j][i];
                    }
				}
				else
                {
                    for (i = 0; i < newNode[j][0].level; i++)
                    {
                        newNode[j][0].next[i] = n[j].next[i];
                    }
                }
				
				for(i=0; i < newNode[j][0].level; i++)
                {
                    pa[j][i].next[i] = newNode[j][0];
                }
                if (split[j] && (newNode[j][1].level > newNode[j][0].level)){
                    for(; i < newNode[j][1].level; i++)
                    { 	
                        pa[j][i].next[i] = newNode[j][1];
                    }
                }

			}
		}
		
	}
	
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
	
	    LeapNode[][]  pa = new LeapNode[size][LeapList.MAX_LEVEL];
	    LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
	    LeapNode[] n = new LeapNode[size];
	    LeapNode[][] oldNode = new LeapNode[size][2];
	    int j;
	    boolean[] changed = new boolean[size], merge = new boolean[size];
	   

	    for(j=0; j<size; j++)
	    {
	        n[j] = new LeapNode();
	        keys[j]+=2; // Avoid sentinel
	    }
	    RemoveSetup(ll,keys, size, pa, na, n, oldNode, merge, changed);
	    RemoveLT(size,pa,na,n,oldNode,merge,changed);
	   /* for(j=0; j<MAX_ROW; j++)
	    {
	       // init_node_trie(n[j]); TODO new trie() and run in ctor new LeapNode()
	    }*/
	    RemoveReleaseAndUpdate(size,pa,na,n,oldNode,merge,changed);
	    
	}

	private void RemoveLT(int size, LeapNode[][] pa, LeapNode[][] na,
			LeapNode[] n, LeapNode[][] oldNode, boolean[] merge,
			boolean[] changed) {
		// TODO Auto-generated method stub
		
	}

	private void RemoveReleaseAndUpdate(int size, LeapNode[][] pa,
			LeapNode[][] na, LeapNode[] n, LeapNode[][] oldNode,
			boolean[] merge, boolean[] changed) {
		// TODO Auto-generated method stub
		 for(int j=0; j<size; j++)
		    {
		        if(changed[j])
		        {
		            // Update the next pointers of the new node
		            int i=0;
		            if (merge[j])
		            {   
		                for (; i < oldNode[j][1].level; i++)
		                    n[j].next[i] = oldNode[j][1].next[i].UnMark();
		            }
			            for (; i < oldNode[j][0].level; i++)
			                n[j].next[i] = oldNode[j][0].next[i].UnMark();
		            
		            
		            for(i = 0; i < n[j].level; i++)
		            {   
		                pa[j][i].next[i] = n[j];
		            }
		            n[j].live = true;
		            if(merge[j])
		            	oldNode[j][1].trie=null;

		            oldNode[j][0].trie=null;
		        }
		        else
		        {
		            n[j].trie=null;
		        }    
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

	void RemoveSetup(LeapList[] ll, long[] keys,int size, LeapNode[][] pa,
			LeapNode[][] na, LeapNode[] n, LeapNode[][] oldNode,
			boolean[] merge, boolean[] changed) 
	{

		int [] total = new int[size];
		for(int j=0; j<size; j++)
	    {
	        merge[j] = false;
	        ll[j].searchPredecessor( keys[j], pa[j], na[j]);
	        oldNode[j][0] = na[j][0];
	        /* If the key is not present, just return */
	        if (find(oldNode[j][0], keys[j]) == null)
	        {
	            changed[j] = false;
	            continue;
	        }
	        total[j] = oldNode[j][0].count;
	        do
	        {
	            oldNode[j][1] = oldNode[j][0].next[0];
	        } while ((oldNode[j][0].live) /*&& (is_marked_ref(oldNode[j][1]))*/);
	       
	        if(oldNode[j][1] != null)
	        {
	            total[j] = total[j] + oldNode[j][1].count;
	            if(total[j] <= LeapList.NODE_SIZE)
	            {
	                merge[j] = true; 
	            }
	        }
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
