package leapListReg;

import java.util.concurrent.ThreadLocalRandom;
import org.deuce.Atomic;
import org.deuce.transaction.TransactionException;

import utils.Trie;


public class LeapListDB {
	
	public static final  int MAX_ROW;
	public LeapList[] LeapLists = new LeapList[MAX_ROW];

	
	public LeapListDB (int numberOfLists) 
	{
		MAX_ROW = numberOfLists;
		LeapLists = new LeapList[MAX_ROW];
		for (int i=0; i < MAX_ROW ; i++)
		{
			LeapLists[i] = new LeapList();
		}
	}
	
	/*function gets input int index and return the list from the 
	 * lists array in the appropriate index, if there is not such lists return null;
	 */
	public LeapList  GetListByIndex (int index)
	{
		if (index < MAX_ROW)
		{
			return LeapLists[index];
		}
		else
		{
			return null;
		}
	}
	
	/*the function return byte that represent the level for the node.
	 * the chance to get level = i is (1/2)^i
	 */
	private byte getLevel()
	{
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
	 * the functio gets Node and key and return the value object if the key 
	 * is in the Node and if not it returns null.
	 */
	private Object find(LeapNode node,long key){
		
		if(node!=null)
		{
			if (node.count > 0)
	        {
				try
				{
		            short indexRes = node.trie.trieFindVal(key);
		            if (indexRes != -1)
		            {
		                return node.data[indexRes].value;
		            }
				}
				catch(NullPointerException e)
				{
					return null;
				}
	        }
	    }
	    return null;		
	}
	
	/*
	 * the function gets LeapList and key and return the value object 
	 * associate with the key in the list or null if the key don't exist in the list
	 */
	public Object lookUp (LeapList l, long key)
	{
		return l.lookUp(key);
	}
	
	/*
	 * the function gets LeapList low key and high key 
	 * and returns set of objects values that associated with keys from the given range.
	 */
	public Object[] RangeQuery (LeapList l,long low, long high)
	{
		return l.RangeQuery(low, high);
	}

	/*
	 * the function gets set of LeapList, set of keys, set of values and the sets size.
	 * and update/insert the key[i] value[i] pair in LeapList[i]
	 */
	public void leapListUpdate (LeapList [] ll, long [] keys, Object [] values, int size)
	{
		for(int i=0;i< size ; i++)
		{
			LeapNode[][] pa = new LeapNode[size][LeapList.MAX_LEVEL];
			LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
			LeapNode[] n = new LeapNode[size];
			LeapNode [][] newNode = new LeapNode[size][2];
			int [] maxHeight = new int[size];
			boolean [] split = new boolean [size];
			boolean [] changed = new boolean [size];
			Boolean stopLoop = true;
			
			keys[i] += 2 ; // avoid sentinel; 
		
				do
				{
					newNode[i][0] = new LeapNode();
					newNode[i][1] = new LeapNode();
					stopLoop = true;
					updateSetup (ll, keys, values, size, pa, na, n, newNode, maxHeight, split, changed,i);
					try
					{
						updateLT (size, pa, na, n, newNode, maxHeight, changed,stopLoop,i);
					}
					catch(TransactionException e)
					{
						stopLoop=false;
						continue;
					}
					updateRelease (size, pa, na, n, newNode,maxHeight, split, changed,i);
				}while(!stopLoop);
			}
			
	}
	
	/*
	 *the function gets LeapList[], keys[], values[], size, LeapNode[][] pa, LeapNode[][] na, 
	 *LeapNode[] n, LeapNode [][] newNode, maxHeight[], boolean split[], boolean changed[] and current list index i.
	 *the function use the functions searchPredecessor and insert to update the inputs and the inputs fields.
	 */
	private void updateSetup (LeapList[] ll, long [] keys, Object [] values, int size, LeapNode[][] pa, LeapNode[][] na, 
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
	
	/*
	 * update the fields of the newNode input from the fields of the input of node n
	 * in the correct way by the input boolean split.
	 */
	private boolean insert (LeapNode[] newNode, LeapNode n, long key, Object val, boolean split)
	{
		boolean changed = false;
		int m = 0;
		int i = 0;
		int j = 0;
	
		
		if (split)
		{//update the two new node
			newNode[0].low = n.low;
			newNode[0].count = (LeapList.NODE_SIZE/2);
			newNode[1].high = n.high;
			newNode[1].count = n.count - (LeapList.NODE_SIZE/2);
		}
		else
		{//update only one new node
			newNode[0].low = n.low;
			newNode[0].high = n.high;
			newNode[0].count = n.count;
		}
		if (n.count == 0)
		{
			newNode[m].data[0].key = key;
			newNode[m].data[0].value = val;
			newNode[m].count = 1;
			changed = true;
			newNode[m].trie = new Trie(key, (short)0);
		}
		else
		{
			for (j = 0; j < n.count; i++, j++)
			{
				if (n.data[j].key == key)
				{     //there is an int overwrite in the prof. cod. not sure what it does.
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = val;
					changed = true;
				}
				else
				{
					
					if ((!changed) && (n.data[j].key > key))
					{
						newNode[m].data[i].key = key;
						newNode[m].data[i].value = val;
						newNode[m].count++;
						changed = true;
						
						// Count = i+1 . if we put the new key in the last place of the node (we know that it's the last place because of the split)
						if ((m != 1) && split && (newNode[0].count == (i+1)))
						{
							newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
							i = -1;
							m = m + 1;
						}
						
						i++;
					}
					
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = n.data[j].value;
				}
				if ((m != 1) && split && (newNode[0].count == (i+1)))
				{
					newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
					i = -1;
					m = m+1;
				}
			}
			
			if (!changed)
			{
				newNode[m].count++;
				newNode[m].data[i].key = key;
				newNode[m].data[i].value = val;
				changed = true;
			}
			
			if (split)
			{
				newNode[0].trie = new Trie(newNode[0].data, newNode[0].count);
				newNode[1].trie = new Trie(newNode[1].data, newNode[1].count);
			}
			else
			{
				newNode[m].trie = new Trie(newNode[m].data, newNode[m].count);
			}
		}
		return changed;
	}
	
	/*
	 * this function use the deuce Stm java agent and it trying to mark all the nodes
	 * we need to change atomically and if it didn't succeed then it roll back 
	 * and we throw TransactionException that we catch outside in leapListUpdate and start trying
	 * all over again for this specific LeapList.
	 */
	@Atomic(retries=1)
	private void updateLT (int size, LeapNode [][] pa, LeapNode [][] na, LeapNode[] n, LeapNode[][] newNode, int[] maxHeight,
								boolean[] changed,Boolean stopLoop,int j) throws TransactionException  {
		int i;
        if (n[j].live == false){
           throw new TransactionException();
        }
        
        for(i = 0; i < n[j].level; i++)
        {   
            if(pa[j][i].getNext(i) != n[j]) 
            {
            	throw new TransactionException();
            }
            if(n[j].getNext(i)!=null)
            {
            	if(!n[j].getNext(i).live) 
            	{
            		throw new TransactionException();
            	}
            }
        }

        for(i = 0; i < maxHeight[j]; i++)
        {   
            if(pa[j][i].getNext(i) != na[j][i])
            {
            	throw new TransactionException();
            }
            if(!(pa[j][i].live)) 
            {
            	throw new TransactionException();
            }
            if(!(na[j][i].live))
            {
            	throw new TransactionException();
            }
        }



        if(changed[j]) // mark all the nodes we need that represent the "lock"
        {
        	for(i = 0; i < n[j].level; i++)
            {
        		if (n[j].getNext(i) != null)
                {
        			if (n[j].Marks[i])
        			{
        				throw new TransactionException();
        			}
        			n[j].Marks[i] = true;
                }
            }
            for(i = 0; i < maxHeight[j]; i++)
            {
            	if (pa[j][i].Marks[i])
            	{
            		throw new TransactionException();
            	}
            	pa[j][i].Marks[i] = true;
            }
            n[j].live = false;
        }
	}

	/*
	 *the function do the actually list changes and then unmark all the nodes that we marked. 
	 */
	private void updateRelease (int size, LeapNode[][] pa, LeapNode[][] na, LeapNode[] n, LeapNode[][] newNode, int[] maxHeight, boolean[] split,
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
					 for (; i < newNode[j][1].level; i++)
					 {
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
                  
                    for (; i < newNode[j][0].level; i++)
                    {
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
            if (split[j])
            {
            	newNode[j][1].live = true;	
            }
		}
	}

	/*
	 * the function gets set of LeapList, set of keys, and the sets size.
	 * and remove the key[i] and the value[i] that associated with key[i] in LeapList[i]
	 */
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{

	    for(int j=0; j<size; j++)
        {
		    LeapNode[][]  pa = new LeapNode[size][LeapList.MAX_LEVEL];
		    LeapNode[][] na = new LeapNode[size][LeapList.MAX_LEVEL];
		    LeapNode[] n = new LeapNode[size];
		    LeapNode[][] oldNode = new LeapNode[size][2];
		    boolean[] changed = new boolean[size], merge = new boolean[size];
		    boolean stopLoop = true;
		    
		    keys[j]+=2; // Avoid sentinel
		    	
		    	do{
		    		n[j] = new LeapNode();
			    	stopLoop=true;
				    RemoveSetup(ll,keys, size, pa, na, n, oldNode, merge, changed,j);
				    try
				    {
				    	RemoveLT(size,pa,na,n,oldNode,merge,changed,stopLoop,j);
				    }
				    catch(TransactionException e)
				    {
				    	stopLoop = false;
				    	continue;
				    }
				    RemoveReleaseAndUpdate(size,pa,na,n,oldNode,merge,changed,j);
	        	}while(!stopLoop);
	        }
	}

	/*
	 *the function gets LeapList[], keys[], values[], size, LeapNode[][] pa, LeapNode[][] na, 
	 *LeapNode[] n, LeapNode [][] newNode, maxHeight[], boolean split[], boolean changed[] and current list index i.
	 *the function use the functions searchPredecessor and remove to update and setup the inputs and the inputs fields.
	 */
	private void RemoveSetup(LeapList[] ll, long[] keys,int size, LeapNode[][] pa,
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
	            if(!oldNode[j][0].live)
	            {
	            	lastRemove=true;
	            	break;
	            }
	        }while (oldNode[j][0].Marks[0]);
	        if(lastRemove || !oldNode[j][0].live)
	        {
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
	        
	        if(!oldNode[j][0].live)
	        {
            	lastRemove=true;
            	continue;
            }
	        
	        if (merge[j] && !oldNode[j][1].live)
	        {
	        	lastRemove=true;
            	continue;
	        }
	        changed[j] = remove(oldNode[j], n[j], keys[j], merge[j]);
		
		}while(lastRemove);
	}	
	
	/*
	 *update the fields of the n input from the fields of the input of old_node
	 *in the correct way by the input boolean merge.
	 */
	private boolean remove(LeapNode[] old_node, LeapNode n,
			 long k, boolean merge) {
	int i,j;
	boolean changed = false;
	
	for (i=0,j=0; j<old_node[0].count; j++)
	{
		if(old_node[0].data[j].key != k)
		{
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
	 *this function use the deuce Stm java agent and it trying to mark all the nodes
	 * we need to change atomically and if it didn't succeed then it roll back 
	 * and we throw TransactionException that we catch outside in leapListRemove and start trying
	 * all over again for this specific LeapList.
	 */
	@Atomic(retries = 1)
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
	
	/*
	 *the function do the actually list changes and then unmark all the nodes that we marked. 
	 */
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

}
