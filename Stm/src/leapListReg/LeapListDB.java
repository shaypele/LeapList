package leapListReg;




import java.util.Random;

import org.deuce.Atomic;
import org.deuce.transaction.TransactionException;

import utils.Trie;


public class LeapListDB {
	public static final  int MAX_ROW = 10;
	public LeapList[] LeapLists = new LeapList[MAX_ROW];

	
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
		Boolean stopLoop = true;
		long[] myKeys = keys.clone();
		
		do{
			keys = myKeys.clone();
			stopLoop = true;
			LeapNode[][] pa = new LeapNode[size][LeapList.MAX_LEVEL];
			LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
			LeapNode[] n = new LeapNode[size];
			LeapNode [][] newNode = new LeapNode[size][2];
			int [] maxHeight = new int[size];
			boolean [] split = new boolean [size];
			boolean [] changed = new boolean [size];
			//Boolean stopLoop = true;
			
			for(int i = 0; i < size; i++){
				newNode[i][0] = new LeapNode();
				newNode[i][1] = new LeapNode();
				keys[i] += 2 ; // avoid sentinel; 
			}
			
			for(int i=0;i< size ; i++)
			{
				
				updateSetup (ll, keys, values, size, pa, na, n, newNode, maxHeight, split, changed,i);
				try
				{
					updateLT (size, pa, na, n, newNode, maxHeight, changed,stopLoop,i);
				}
				catch(TransactionException e){
					//System.out.print("in catch " + Retry + "\n");
				//	Retry = 0;
					stopLoop=false;
					break;
				}
				updateRelease (size, pa, na, n, newNode,maxHeight, split, changed,i);
			}
		}while(!stopLoop);
			
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
										LeapNode[] n, LeapNode [][] newNode, int [] maxHeight, boolean[] split, boolean[] changed,int i){
		
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
	int Retry = 0;
	
	@Atomic(retries=1)
	void updateLT (int size, LeapNode [][] pa, LeapNode [][] na, LeapNode[] n, LeapNode[][] newNode, int[] maxHeight,
								boolean[] changed,Boolean stopLoop,int j) throws TransactionException  {
		int i;
		//Retry  ++;
		//System.out.print("in upadtelt \n");
		//if (1  > 0)
		//throw new TransactionException();
        if (n[j].live == false)
           throw new TransactionException();

        for(i = 0; i < n[j].level; i++)
        {   
            if(pa[j][i].getNext(i) != n[j]) {
            	throw new TransactionException();
            }
            if(n[j].getNext(i)!=null){
            	if(!n[j].getNext(i).live) {
            		throw new TransactionException();
            	}
            }
        }

        for(i = 0; i < maxHeight[j]; i++)
        {   
            if(pa[j][i].getNext(i) != na[j][i]){
            	throw new TransactionException();
            }
            if(!(pa[j][i].live)) {
            	throw new TransactionException();
            }
            if(!(na[j][i].live)){
            	throw new TransactionException();
            }
        }



        if(changed[j]) // lock
        {
        	for(i = 0; i < n[j].level; i++)
            {
        		if (n[j].getNext(i) != null)
                {
        			if (n[j].Marks[i]){
        				throw new TransactionException();
        			}
        			n[j].Marks[i] = true;
                }
            }
            for(i = 0; i < maxHeight[j]; i++)
            {
            	if (pa[j][i].Marks[i]){
            		throw new TransactionException();
            	}
            	pa[j][i].Marks[i] = true;
            }
            n[j].live = false;
            	
        }

	}

	void updateRelease (int size, LeapNode[][] pa, LeapNode[][] na, LeapNode[] n, LeapNode[][] newNode, int[] maxHeight, boolean[] split,
									boolean[] changed,int j){
		
		int i = 0;
		
		if (changed[j])
		{
			if (split[j])
			{
				if (newNode[j][1].level > newNode[j][0].level)
				{
					for (i = 0; i < newNode[j][0].level; i++)
					{
						newNode[j][0].setNext(i, newNode[j][1]) ;
                        newNode[j][1].setNext(i, n[j].getNext(i));
                        n[j].Marks[i] = false ;
					}
					 for (; i < newNode[j][1].level; i++){
						 newNode[j][1].setNext(i,n[j].getNext(i));
						 n[j].Marks[i] = false ;
					 }
				}
				else
                {   
                    for (i = 0; i < newNode[j][1].level; i++)
                    {
                    	newNode[j][0].setNext(i, newNode[j][1]);
                        newNode[j][1].setNext(i, n[j].getNext(i));
                        n[j].Marks[i] = false ;
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
                	 n[j].Marks[i] = false ;
                }
            }
			
			for(i=0; i < newNode[j][0].level; i++)
            {
				pa[j][i].setNext(i, newNode[j][0]);
				pa[j][i].Marks[i] = false;
            }
            if (split[j] && (newNode[j][1].level > newNode[j][0].level))
            {
                for(; i < newNode[j][1].level; i++)
                { 	
                	pa[j][i].setNext(i, newNode[j][1]);
                	pa[j][i].Marks[i] = false;
                }
            }
            
            newNode[j][0].live = true;
            if (split[j]){
            	
            	newNode[j][1].live = true;
            	
            }
		}
		
	}
	
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
		Boolean stopLoop = true;
		long[] myKeys = keys.clone();

		do{
			keys = myKeys.clone();
			stopLoop = true;
			LeapNode[][]  pa = new LeapNode[size][LeapList.MAX_LEVEL];
		    LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
		    LeapNode[] n = new LeapNode[size];
		    LeapNode[][] oldNode = new LeapNode[size][2];
			boolean [] merge = new boolean [size];
			boolean [] changed = new boolean [size];
			//Boolean stopLoop = true;
		
			for(int i = 0; i < size; i++){	
				keys[i] += 2 ; // avoid sentinel; 
				n[i] = new LeapNode();
			}
			
	    	for (int j = 0; j < size; j++) {
				
			    RemoveSetup(ll,keys, size, pa, na, n, oldNode, merge, changed,j);
			    try
			    {
			    	RemoveLT(size,pa,na,n,oldNode,merge,changed,stopLoop,j);
			    }
			    catch(TransactionException e)
			    {
			    	stopLoop = false;
			    	break;
			    }
			    RemoveReleaseAndUpdate(size,pa,na,n,oldNode,merge,changed,j);
		        }
        }while(!stopLoop);
	}
	
	@Atomic(retries = 4)
	private void RemoveLT(int size, LeapNode[][] pa, LeapNode[][] na,
			LeapNode[] n, LeapNode[][] oldNode, boolean[] merge,
			boolean[] changed,boolean stopLoop, int j) 
	{
		int i=0;
        if(changed[j])
        {
            if (!oldNode[j][0].live)
            {
            	throw new TransactionException();
            }
            if (merge[j] && !oldNode[j][1].live)
            {
            	throw new TransactionException();
            }
            
            for(i = 0; i < oldNode[j][0].level;i++)
            {
                if (pa[j][i].getNext(i) != oldNode[j][0]) 
                {
                	throw new TransactionException();
                }
                if (!(pa[j][i].live)) 
                {
                	throw new TransactionException();
                }
                if (oldNode[j][0].getNext(i) != null)
                {
                	if (!oldNode[j][0].getNext(i).live)
                	{
                		throw new TransactionException();
                	}
                }
            }


            if (merge[j])
            {   
                // Already checked that old_node[0]->next[0] is live, need to check if they are still connected
                if (oldNode[j][0].getNext(0) != oldNode[j][1])
                	throw new TransactionException();

                if (oldNode[j][1].level > oldNode[j][0].level)
                {   
                    // Up to old_node[0] height, we only need to validate the next nodes of old_node[1]
                    for (i = 0; i < oldNode[j][0].level; i++)
                    {
                        if (oldNode[j][1].getNext(i)!=null){
                        	if (!oldNode[j][1].getNext(i).live) {
                        		throw new TransactionException();
                        	}
                        }
                    }
                    // For the higher part, we need to check also the pa of that part
                    for (; i < oldNode[j][1].level; i++)
                    {
                        if (pa[j][i].getNext(i) != oldNode[j][1]) {
                        	throw new TransactionException();
                        }
                        if (!(pa[j][i].live)) {
                        	throw new TransactionException();
                        }
                        if (oldNode[j][1].getNext(i)!=null) {
                        	if (!oldNode[j][1].getNext(i).live){
                        		throw new TransactionException();
                        	}
                        }
                    }

                }
                else // oldNode[0] is higher than oldNode[1], just check the next pointers of oldNode[1]
                {
                    for (i = 0; i < oldNode[j][1].level; i++)
                    {
                        if (oldNode[j][1].getNext(i)!=null){
                        	if (!oldNode[j][1].getNext(i).live){
                        		throw new TransactionException();
                        	}
                        }
                    }
                }
            }

            // Lock the pointers to the next nodes
            if(merge[j])
            {
                for(i = 0; i < oldNode[j][1].level; i++)
                {
                    if (oldNode[j][1].getNext(i) != null)
                    {   
                        if(oldNode[j][1].Marks[i] == true) {
                        	throw new TransactionException();
                        }
                        oldNode[j][1].Marks[i] = true;
                    }
                }
                for(i = 0; i < oldNode[j][0].level; i++)
                {
                    if (oldNode[j][0].getNext(i) != null)
                    {   
                        if(oldNode[j][0].Marks[i] == true) {
                        	throw new TransactionException();
                        }
                        oldNode[j][0].Marks[i] = true;
                    }
                }
            }
            else
            {   
                for(i = 0; i < oldNode[j][0].level; i++)
                {
                    if (oldNode[j][0].getNext(i) != null)
                    {   
                        if(oldNode[j][0].Marks[i] == true) {
                        	throw new TransactionException();
                        }
                        oldNode[j][0].Marks[i] = true;
                    }
                }
            }

            // Lock the pointers to the current node
            for(i = 0; i < n[j].level; i++)
            {
                if(pa[j][i].Marks[i] == true){
                	throw new TransactionException();
                }
                pa[j][i].Marks[i] = true;
            }

            oldNode[j][0].live=false;
            if (merge[j])
            {
                oldNode[j][1].live=false;	
            }
        }
	}

	private void RemoveReleaseAndUpdate(int size, LeapNode[][] pa,
			LeapNode[][] na, LeapNode[] n, LeapNode[][] oldNode,
			boolean[] merge, boolean[] changed, int j) {

	        if(changed[j])
	        {
	            // Update the next pointers of the new node
	            int i=0;
	            if (merge[j])
	            {   
	                for (; i < oldNode[j][1].level; i++)
	                {
	                	n[j].setNext(i, oldNode[j][1].getNext(i));
	                	oldNode[j][1].Marks[i] =false;
	                }
	            }
	            for (; i < oldNode[j][0].level; i++){
	            	n[j].setNext(i, oldNode[j][0].getNext(i));
	            	oldNode[j][0].Marks[i] =false;
	            }
	            
	            for(i = 0; i < n[j].level; i++)
	            {   
	            	 pa[j][i].setNext(i, n[j]);
	            	 pa[j][i].Marks[i] =false;
	            }
	            
	            n[j].live = true;
	            
	            if(merge[j])
	            {
	            	oldNode[j][1].trie=null;
	            }
	            oldNode[j][0].trie=null;
	        }
	        else
	        {
	            n[j].trie=null;
	        }    
	}
	
	Object find(LeapNode node,long key){
		
		if(node!=null){
			if (node.count > 0)
	        {
				try{
	            short indexRes = node.trie.trieFindVal(key);
	            if (indexRes != -1)
	            {
	                return node.data[indexRes].value;
	            }
				}
				catch(NullPointerException e){
					return null;
				}
	        }
	    }
	    return null;
		
	}

	void RemoveSetup(LeapList[] ll, long[] keys,int size, LeapNode[][] pa,
			LeapNode[][] na, LeapNode[] n, LeapNode[][] oldNode,
			boolean[] merge, boolean[] changed, int j) 
	{
		boolean lastRemove=false;
		int [] total = new int[size];
		
			do{
				lastRemove=false;
		        merge[j] = false;
		        ll[j].searchPredecessor( keys[j], pa[j], na[j]);
		        oldNode[j][0] = na[j][0];
		        // If the key is not present, just return 
		        if (find(oldNode[j][0], keys[j]) == null)
		        {
		            changed[j] = false;
		            continue;
		        }
		        
		        do
		        {
		            oldNode[j][1] = oldNode[j][0].getNext(0);
		            if(!oldNode[j][0].live){
		            	lastRemove=true;
		            	break;
		            }
		        } while (oldNode[j][0].Marks[0]);
		        if(lastRemove  || !oldNode[j][0].live){
		        	lastRemove=true;
		        	continue;
		        }
		        
		        total[j] = oldNode[j][0].count;
		        
		        if(oldNode[j][1] != null)
		        {
		            total[j] = total[j] + oldNode[j][1].count;
		            if(total[j] - 1<= LeapList.NODE_SIZE)
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
		        
		        if(!oldNode[j][0].live){
	            	lastRemove=true;
	            	continue;
	            }
		        
		        if (merge[j] && !oldNode[j][1].live){
		        	lastRemove=true;
	            	continue;
		        }
		        changed[j] = remove(oldNode[j], n[j], keys[j], merge[j]);
			
			}while(lastRemove);
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
