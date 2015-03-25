package leapListReg;
import java.util.concurrent.ThreadLocalRandom;
import org.deuce.Atomic;
import org.deuce.transaction.TransactionException;

import utils.Trie;
/*
 * LeapListDB class represent the L LeapList data structure and it has an array of LeapLists.
 */

public class LeapListDB {
	
	static int MAX_ROW ;
	public LeapList[] LeapLists;

	
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
	 *function gets input int index and return the list from the 
	 * lists array in the appropriate index, if there is not such lists return null;
	 */
	public LeapList  GetListByIndex (int index){
		if (index < MAX_ROW)
		{
			return LeapLists[index];
		}
		else
		{
			return null;
		}
	}
	
	/*
	 *the function return byte that represent the level for the node.
	 * the chance to get level = i is (1/2)^i
	 */
	private byte getLevel(){
		long r = ThreadLocalRandom.current().nextLong();
		byte l = 1;
		r = (r >> 4) & ((1 << (LeapList.MAX_LEVEL - 1)) -1);
		while ((r & 1)  > 0)
		{
			l++;
			r >>= 1;
		}
		return l;
	}
	
	/*
	 *the function gets Node and key and return the value object if the key 
	 * is in the Node and if not it returns null.
	 */
	private Object find(LeapNode node,long key){
		
		if(node!=null)
		{
			if (node.count > 0)
	        {
				try
				{
					//call the trieFindVal function from the node's trie
		            short indexRes = node.trie.trieFindVal(key);
		            if (indexRes != -1)
		    		{//if found get the value from the index
		                return node.data[indexRes].value;
		            }
				}
				catch(NullPointerException e)
				{//if between we found the key other thread erase the node
					return null;
				}
	        }
	    }
	    return null;	
	}
	
	/*
	 *the function gets LeapList and key and return the value object 
	 * associate with the key in the list or null if the key don't exist in the list
	 */
	public Object lookUp (LeapList l, long key){
		//call the LeapList lookUp with the given key
			return l.lookUp(key);
	}
	
	/*
	 *the function gets LeapList low key and high key 
	 * and returns set of objects values that associated with keys from the given range.
	 */
	public Object[] RangeQuery (LeapList l,long low, long high){
		// call the LeapList RangeQuery function with the give low and high
		return l.RangeQuery(low, high);
	}
	
	/*
	 *the function gets set of LeapList, set of keys, set of values and the sets size.
	 * and update/insert the key[i] value[i] pair in LeapList[i], by calling updateSetup(), updateLT() and updateRelease().
	 */
	public void leapListUpdate (LeapList [] ll, long [] keys, Object [] values, int size){
		Boolean stopLoop = true;
		//newKeys is the actual keys we put in the lists after we adding them 2 to avoid the sentinel keys 0 and 1.
		long[] newKeys = new long[size];
		// pa, na - each pa and na is filled by search_predeccessors
		LeapNode[] pa = new LeapNode[LeapList.MAX_LEVEL];
		LeapNode[] na = new LeapNode[LeapList.MAX_LEVEL];
		LeapNode[] n = new LeapNode[size];
		LeapNode[] newNode = null;
		
		do{
			stopLoop = true;
			//maxHeight[i] holds highest level of the nodes in list[i] in case the node split else it holds the level of the node.
			int [] maxHeight = new int[size];
			//if split[i] is true there is a split for the node in list[i].
			boolean [] split = new boolean [size];
			//if change[i] is true it means that we add/change the key value pair in list[i].
			boolean [] changed = new boolean [size];
			
			for(int i = 0; i < size; i++)
			{
				newKeys[i] = keys[i] + 2 ; // avoid sentinel; 
			}
			//run over all the LeapLists
			for(int i=0;i< size ; i++)
			{	
				//create all the variables for the current list
				pa = new LeapNode[LeapList.MAX_LEVEL];
				na = new LeapNode[LeapList.MAX_LEVEL];
				// the newNode is an array of size two for the cases we need to split the node.
				newNode = new LeapNode[]{new LeapNode(),new LeapNode()};
				updateSetup (ll[i], newKeys[i], values[i], size, pa, na, n, newNode, maxHeight, split, changed,i);
				try
				{//if updateLT throw TransactionException it means it didn't run atomically and rolled back
				//so we restart the loop and run over all the LeapLists again until we succeed.
					updateLT (size, pa, na, n[i], newNode, maxHeight, changed,stopLoop,i);
				}
				catch(TransactionException e){
					stopLoop=false;
					break;
				}
				updateRelease (size, pa, na, n[i], newNode,maxHeight, split, changed,i);
			}
		}while(!stopLoop);
	}

	/*
	 *the function gets LeapList[], keys[], values[], size, LeapNode[][] pa, LeapNode[][] na, 
	 *LeapNode[] n, LeapNode [][] newNode, maxHeight[], boolean split[], boolean changed[] and current list index i.
	 *the function use the functions searchPredecessor and insert to update the inputs and the inputs fields.
	 */
	private void updateSetup (LeapList l, long key, Object value, int size, LeapNode[] pa, LeapNode[] na, 
										LeapNode[] n, LeapNode[] newNode, int [] maxHeight, boolean[] split, boolean[] changed,int i){
		
			// Get the predecessors (in all of the levels ) of the node that key should be added to into pa, get the successors of pa into na.
			// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node (or 2 new nodes is split is required), this node is returned
			//	into n.
			l.searchPredecessor(key, pa, na);	
			n[i] = na[0];
			// If the node to be removed has reached its maximum size, it should be split into 2 new nodes.Otherwise only one node will replace it.
			if (n[i].count == LeapList.NODE_SIZE)
			{
				split[i] = true;
				newNode[1].level = n[i].level;
				newNode[0].level = getLevel();
				maxHeight[i] = (newNode[0].level > newNode[1].level) ? newNode[0].level: newNode[1].level;
			}
			else
			{
				split[i] = false;
				newNode[0].level = n[i].level;
				maxHeight[i] = newNode[0].level;
			}
			// Copy values of the old node to the new node ( or 2 if there was a split ).
			changed [i] = insert(newNode, n[i], key, value, split[i]);
	}
	
	/*
	 *update the fields of the newNode input from the fields of the input of node n
	 * in the correct way by the input boolean split.
	 */
	private boolean insert (LeapNode[] newNode, LeapNode n, long key, Object val, boolean split)
	{
		boolean changed = false;
		int m = 0;
		int i = 0;
		int j = 0;
	
		// If there's a split, put NODE_SIZE/2 elements in one node and the rest in the other.
		// The 2 new nodes are now associated with the key range that was associated with one node 'n'
		if (split)
		{
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
				// If key was found in old node update it's value and assign it to the new node.
				//Otherwise , add it like a normal key-value pair from node 'n'
				if (n.data[j].key == key)
				{     //there is an int overwrite in the prof. cod. not sure what it does.
					newNode[m].data[i].key = n.data[j].key;
					newNode[m].data[i].value = val;
					changed = true;
				}
				else
				{
					// Data is a sorted array therefore if next element has a bigger key then the given key 'k', that's is the place to put the given key-value pair into.
					if ((!changed) && (n.data[j].key > key))
					{
						newNode[m].data[i].key = key;
						newNode[m].data[i].value = val;
						newNode[m].count++;
						changed = true;
						
						// Move to next node if split == 1 and new_node has reached it's assigned capcity 			
						if ((m != 1) && split && (newNode[0].count == (i+1)))
						{
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
				if ((m != 1) && split && (newNode[0].count == (i+1)))
				{
					newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
					i = -1;
					m = m+1;
				}
			}
			// In case the given key-value pair should be inserted in the end of the array, insert it here.
			if (!changed)
			{
				newNode[m].count++;
				newNode[m].data[i].key = key;
				newNode[m].data[i].value = val;
				changed = true;
			}
			
			if (split)
			{/* build the tries for the new nodes to allow quick access to the key's index in the data array.*/
				newNode[0].trie = new Trie(newNode[0].data, newNode[0].count);
				newNode[1].trie = new Trie(newNode[1].data, newNode[1].count);
			}
			else
			{/* build the trie for the new node to allow quick access to the key's index in the data array.*/
				newNode[m].trie = new Trie(newNode[m].data, newNode[m].count);
			}
		}
		return changed;
	}
	
	/*
	 *this function use the deuce Stm java agent and it trying to mark all the nodes
	 * we need to change atomically and if it didn't succeed then it roll back 
	 * and we throw TransactionException that we catch outside in leapListUpdate and start trying
	 * all over again for all the LeapLists.
	 * we try this function only once and if it not succeed we start trying all over again for all the LeapLists,
	 * after check several numbers of retries 1 gives the best performance because in the most cases we need to use searchPredecessor again
	 * because the list is changed.
	 */
	@Atomic(retries=1)
	private void updateLT (int size, LeapNode[] pa, LeapNode[] na, LeapNode n, LeapNode[] newNode, int[] maxHeight,
								boolean[] changed,Boolean stopLoop,int j) throws TransactionException  {
		int i;
        if (n.live == false)
        {//if n isn't alive throw TransactionException to restart leapListUpdate loop.
           throw new TransactionException();
        }
        
        //traverse all the levels of node n
        for(i = 0; i < n.level; i++)
        {   
            if(pa[i].getNext(i) != n) 
            {//if predecessor don't point to the node at level i then other thread got in the way
            // then throw TransactionException to restart leapListUpdate loop.
            	throw new TransactionException();
            }
            if(n.getNext(i)!=null)
            {
            	if(!n.getNext(i).live) 
            	{// if n's next in level i is not alive then other thread got in the way
                    // then throw TransactionException to restart leapListUpdate loop.
            		throw new TransactionException();
            	}
            }
        }

        for(i = 0; i < maxHeight[j]; i++)
        {   
            if(pa[i].getNext(i) != na[i])
            {//if predecessor don't point to the successor at level i then other thread got in the way
                // then throw TransactionException to restart leapListUpdate loop.
            	throw new TransactionException();
            }
            if(!(pa[i].live)) 
            {
            	// if pa in level i is not alive then other thread got in the way
                // then throw TransactionException to restart leapListUpdate loop.
            	throw new TransactionException();
            }
            if(!(na[i].live))
            {// if na in level i is not alive then other thread got in the way
                // then throw TransactionException to restart leapListUpdate loop.
            	throw new TransactionException();
            }
        }



        if(changed[j]) // this part of the code represent the locks of the nodes when we mark them
        {
        	for(i = 0; i < n.level; i++)
            {
        		if (n.getNext(i) != null)
                {//check if level i mark so throw TransactionException to restart leapListUpdate loop,
        			//else mark the level i.
        			if (n.Marks[i])
        			{
        				throw new TransactionException();
        			}
        			n.Marks[i] = true;
                }
            }
            for(i = 0; i < maxHeight[j]; i++)
            {//check if level i mark so throw TransactionException to restart leapListUpdate loop,
    			//else mark the level i.
            	if (pa[i].Marks[i])
            	{
            		throw new TransactionException();
            	}
            	pa[i].Marks[i] = true;
            }
            //after we succeed marking all the nodes we can replace the node n so from now on is not live.
            n.live = false; 	
        }
	}

	/*
	 * For each list in l. Link the pointers of the new nodes to point to the successors in na. Also, re-link the pa pointers to point to the new nodes assigned.
	 * After that the new nodes are accessible, they are part of the lists.
	 */
	private void updateRelease (int size, LeapNode[] pa, LeapNode[] na, LeapNode n, LeapNode[] newNode, int[] maxHeight, boolean[] split,
									boolean[] changed,int j){
		
		int i = 0;
		
		if (changed[j])
		{//if we add/change the node
            // First point the next pointers of all level in the new node ( or 2 if there's a split)  to the assoicated successors in succs.
			if (split[j])
			{
				// If there is a split, distinguish between two case: one where new_node[1].level > new_node[0].level and the opposite.
				if (newNode[1].level > newNode[0].level)
				{
					//  Since new_node[1].level = n.level and new_node[1].level > new_node[0].level, point all 'next' pointers in new_node[0] to new_node[1],
					//  Next, copy all 'next' pointers from n to new_node[1] (since they share the same maximum level)
					for (i = 0; i < newNode[0].level; i++)
					{
						newNode[0].setNext(i, newNode[1]) ;
                        newNode[1].setNext(i, n.getNext(i));
					}
					 for (; i < newNode[1].level; i++)
					 {
						 newNode[1].setNext(i,n.getNext(i));
					 }
				}
				else
                {   
					// In this case only point part of new_node[0] 'next' pointers to new_node[1]. The others should point to na[i] , where i represents the higher levels.
					// This is because new_node[0] max level is bigger the original's node n's max level.
                    for (i = 0; i < newNode[1].level; i++)
                    {
                    	newNode[0].setNext(i, newNode[1]);
                        newNode[1].setNext(i, n.getNext(i));
                    }
                  
                    for (; i < newNode[0].level; i++)
                    {
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
            // Link the predecessors pa 'next' pointers of the node n to the new node ( or 2 if there's a split )
			//and unmark the "pointer" in level i 
			for(i=0; i < newNode[0].level; i++)
            {
				pa[i].setNext(i, newNode[0]);
				pa[i].Marks[i] = false;
            }
			// If there's a split and new_node[1].level > new_node[0].level , Link the predecessors pa 'next' pointers of the node n to new node[1]
			//and unmark the "pointer" in level i 
			if (split[j] && (newNode[1].level > newNode[0].level))
            {
                for(; i < newNode[1].level; i++)
                { 	
                	pa[i].setNext(i, newNode[1]);
                	pa[i].Marks[i] = false;
                }
            }
            // Linking completed, mark the new nodes as live.
            newNode[0].live = true;
            if (split[j])
            {
            	newNode[1].live = true;
            }
		}
		
	}
	
	/*
	 * The method receives arrays of LeapLists, keys, values and the size of the arrays.
	 * It tries to get the lock.
	 * If successful it removes the key[i] value[i] pair for LeapList[i] (if exists), by calling RemoveSetup(), RemoveLT and RemoveRelease().
	 */
	public void leapListRemove(LeapList[] ll, long[] keys, int size)
	{
		Boolean stopLoop = true;
		//newKeys is the actual keys we put in the lists after we adding them 2 to avoid the sentinel keys 0 and 1.
		long[] newKeys = new long[size];
		// pa, na - - each pa[i] and na[i] is filled by search_predeccessors
		LeapNode[]  pa = null;
	    LeapNode[] na = null;
	    LeapNode n = null;
	    LeapNode[] oldNode = null;
		do{
			stopLoop = true;
			//if change[i] is true it means that the key is in the list[i] and we remove the key value pair
			//if merge[i] is true there is a merge between the node and his successor in list[i].
			boolean [] merge = new boolean [size];
			boolean [] changed = new boolean [size];
		
			for(int i = 0; i < size; i++)
			{	
				newKeys[i] = keys[i] + 2 ; // avoid sentinel; 
			}
			
			  //run over all the lists
	    	for (int j = 0; j < size; j++) 
	    	{
	    		//create all the variables for the current list
	    		pa = new LeapNode[LeapList.MAX_LEVEL];
	    		na = new LeapNode[LeapList.MAX_LEVEL];
			    //oldNode is the node that we want to remove from him the key value pair.
	    		oldNode = new LeapNode[2];
	    		n = new LeapNode();
			    RemoveSetup(ll,newKeys[j], size, pa, na, n, oldNode, merge, changed,j);
			    try
			    {//if RemoveLT throw TransactionException it means it didn't run atomically and rolled back
					//so we restart the loop and run over all the LeapLists again until we succeed.
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
	
	/*
	 * The method calls the searchPredecessor method on the LeapList and key. It checks if the matching node
	 * contains the desired key. If not it returns a new Node. If the key is found it checks the count field and determines if
	 * the Node needs to be split after the removal of the pair. It than continues to copy the old values to the new Node\s.
	 */
	private void RemoveSetup(LeapList[] ll, long key,int size, LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int j) 
	{
		boolean lastRemove=false;
		int [] total = new int[size];
		
			do{
				lastRemove=false;
		        merge[j] = false;
		        // Get the predecessors (in all of the levels ) of the node that key should be added to into pa, get the successors of pa into na.
				// The successor of the predecessors in the bottom level is the node to be removed and replaces by the new node, this node is returned
				//	into old_node.
		        ll[j].searchPredecessor( key, pa, na);
		        oldNode[0] = na[0];
		        // If the key is not present, just return 
		        if (find(oldNode[0], key) == null)
		        {
		            changed[j] = false;
		            continue;
		        }
		        
		        do
		        {//do until oldNode[0] is live and the next node is not marked.
		            oldNode[1] = oldNode[0].getNext(0);
		            if(!oldNode[0].live)
		            {
		            	lastRemove=true;
		            	break;
		            }
		        } while (oldNode[0].Marks[0]);
		        if(lastRemove  || !oldNode[0].live)
		        {
		        	lastRemove=true;
		        	continue;
		        }
		        total[j] = oldNode[0].count;
		        
		        if(oldNode[1] != null)
		        {
		            total[j] = total[j] + oldNode[1].count;
					// if(total -1 <= NODE_SIZE) a merge is required, so the two nodes will become one new node.
		            if(total[j] - 1<= LeapList.NODE_SIZE)
		            {
		                merge[j] = true; 
		            }
		        }
			     // Copy the old_node's properties to the new one.
		        n.level = oldNode[0].level;    
		        n.low   = oldNode[0].low;
		        n.count = oldNode[0].count;
		        n.live = false;
	
				// If a merge is required, update the new node's properties. 
		        //Get the maximum level between both of the old nodes 
		        //and increase the number of elements and maximum expected key in the new node.
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
		        
		        if(!oldNode[0].live)
		        {//other thread remove the oldNode[0] so restart the loop
	            	lastRemove=true;
	            	continue;
	            }
		        
		        if (merge[j] && !oldNode[1].live)
		        {//other thread remove the oldNode[1] so restart the loop
		        	lastRemove=true;
	            	continue;
		        }
				// Call remove in order to copy all key-value pairs from the old node 
		        //( or 2 nodes if merge is required ) without the key to be removed.
		        changed[j] = remove(oldNode, n, key, merge[j]);
		        
			}while(lastRemove);
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
	 *this function use the deuce Stm java agent and it trying to mark all the nodes
	 * we need to change atomically and if it didn't succeed then it roll back 
	 * and we throw TransactionException that we catch outside in leapListRemove and start trying
	 * all over again for all the LeapLists.
	 * we try this function only once and if it not succeed we start trying all over again for all the LeapLists,
	 * after check several numbers of retries 1 gives the best performance because in the most cases we need to use searchPredecessor again
	 * because the list is changed.
	 */
	@Atomic(retries = 1)
	private void RemoveLT(int size, LeapNode[] pa, LeapNode[] na,
			LeapNode n, LeapNode[] oldNode, boolean[] merge,
			boolean[] changed,boolean stopLoop, int j) 
	{
		int i=0;
        if(changed[j])
        {
            if (!oldNode[0].live)
            {//if oldNode[0] isn't alive throw TransactionException to restart leapListRemove loop.
            	throw new TransactionException();
            }
            if (merge[j] && !oldNode[1].live)
            {//if there was a merge and oldNode[1] isn't alive throw TransactionException to restart leapListRemove loop.
            	throw new TransactionException();
            }
            //traverse all the oldNode[0] levels
            for(i = 0; i < oldNode[0].level;i++)
            {
            	//if predecessor don't point to the oldNode[0] at level i then other thread got in the way
                // then throw TransactionException to restart leapListRemove loop.
                if (pa[i].getNext(i) != oldNode[0]) 
                {
                	throw new TransactionException();
                }
                if (!(pa[i].live)) 
                {//if pa[i] isn't alive throw TransactionException to restart leapListRemove loop.
                	throw new TransactionException();
                }
                if (oldNode[0].getNext(i) != null)
                {
                	if (!oldNode[0].getNext(i).live)
                    {//if oldNode[0] next in level i isn't alive throw TransactionException to restart leapListRemove loop.
                		throw new TransactionException();
                	}
                }
            }


            if (merge[j])
            {   
                // Already checked that old_node[0]->next[0] is live, need to check if they are still connected
                if (oldNode[0].getNext(0) != oldNode[1])
                {//if oldNode[0] next is not oldNode[1] throw TransactionException to restart leapListRemove loop.
                	throw new TransactionException();
                }

                if (oldNode[1].level > oldNode[0].level)
                {   
                    // Up to old_node[0] height, we only need to validate the next nodes of old_node[1]
                    for (i = 0; i < oldNode[0].level; i++)
                    {
                        if (oldNode[1].getNext(i)!=null)
                        {
                        	if (!oldNode[1].getNext(i).live) 
                            {//if oldNode[1] next in level i isn't alive throw TransactionException to restart leapListRemove loop.
                        		throw new TransactionException();
                        	}
                        }
                    }
                    // For the higher part, we need to check also the pa of that part
                    for (; i < oldNode[1].level; i++)
                    {
                        if (pa[i].getNext(i) != oldNode[1]) 
                        {//if pa[i] next in level i not pointing oldNode[1] throw TransactionException to restart leapListRemove loop.
                        	throw new TransactionException();
                        }
                        if (!(pa[i].live)) 
                        {//if pa[i] isn't alive throw TransactionException to restart leapListRemove loop.
                        	throw new TransactionException();
                        }
                        if (oldNode[1].getNext(i)!=null) 
                        {
                        	if (!oldNode[1].getNext(i).live)
                            {//if oldNode[1] next in level i isn't alive throw TransactionException to restart leapListRemove loop.
                        		throw new TransactionException();
                        	}
                        }
                    }

                }
                else // oldNode[0] is higher than oldNode[1], just check the next pointers of oldNode[1]
                {
                    for (i = 0; i < oldNode[1].level; i++)
                    {
                        if (oldNode[1].getNext(i)!=null)
                        {
                        	if (!oldNode[1].getNext(i).live)
                            {//if oldNode[1] next in level i isn't alive throw TransactionException to restart leapListRemove loop.
                        		throw new TransactionException();
                        	}
                        }
                    }
                }
            }

            // this part represent a Lock of the pointers to the next nodes
            if(merge[j])
            {
                for(i = 0; i < oldNode[1].level; i++)
                {
                    if (oldNode[1].getNext(i) != null)
                    {   //check if the "pointer" level i of oldNode[1] mark so throw TransactionException to restart leapListRemove loop,
            			//else mark the level i "pointer".
                        if(oldNode[1].Marks[i] == true) 
                        {
                        	throw new TransactionException();
                        }
                        oldNode[1].Marks[i] = true;
                    }
                }
                for(i = 0; i < oldNode[0].level; i++)
                {
                    if (oldNode[0].getNext(i) != null)
                    {   //check if the "pointer" level i of oldNode[0] mark so throw TransactionException to restart leapListRemove loop,
            			//else mark the level i "pointer".
                        if(oldNode[0].Marks[i] == true) 
                        {
                        	throw new TransactionException();
                        }
                        oldNode[0].Marks[i] = true;
                    }
                }
            }
            else
            {   
                for(i = 0; i < oldNode[0].level; i++)
                {
                    if (oldNode[0].getNext(i) != null)
                    {   //check if the "pointer" level i of oldNode[0] mark so throw TransactionException to restart leapListRemove loop,
            			//else mark the level i "pointer".
                        if(oldNode[0].Marks[i] == true) 
                        {
                        	throw new TransactionException();
                        }
                        oldNode[0].Marks[i] = true;
                    }
                }
            }

            // Lock the pointers to the current node
            for(i = 0; i < n.level; i++)
            {
                if(pa[i].Marks[i] == true)
                {//check if the "pointer" level i of pa[i] mark so throw TransactionException to restart leapListRemove loop,
        			//else mark the level i "pointer".
                	throw new TransactionException();
                }
                pa[i].Marks[i] = true;
            }
            //after we succeed marking all the nodes we can replace the node oldNode[0] so from now on is not live.
            oldNode[0].live=false;
            if (merge[j])
            {//if there was merge after we succeed marking all the nodes we can replace the node oldNode[1] so from now on is not live.
                oldNode[1].live=false;	
            }
        }
	}

	/*
	 * Link the 'next' pointers of the node n to point to the successors of the old node. Also, link the predecessors pa pointers to point to the node n .
	 * After that the new nodes are not accessible, and they are not part of the lists.
	 */
	private void RemoveReleaseAndUpdate(int size, LeapNode[] pa,
			LeapNode[] na, LeapNode n, LeapNode[] oldNode,
			boolean[] merge, boolean[] changed, int j) {

	        if(changed[j])
	        {
	            // Update the next pointers of the new node
	            int i=0;
	            if (merge[j])
	            {   
	            	// If a merge is required, get the 'next' pointers from oldNode[1] into the new node 'n'. Later on if oldNode[0] has a higher max level than oldNode[1], node 'n' will get the 'next' pointers from it.
	    			// If no merge is required, copy all next pointers from oldNode[0] into the new node 'n'. 
	                for (; i < oldNode[1].level; i++)
	                {
	                	n.setNext(i, oldNode[1].getNext(i));
	                }
	            }
	            //set the next of  n to be oldNode[0] next at each level 
	            for (; i < oldNode[0].level; i++)
	            {
	            	n.setNext(i, oldNode[0].getNext(i));
	            }
	            // n.level is the max level between oldNode[0] and oldNode[1] ( in case of a merge ), so the merge case is covered here as well.
	            // Link the predecessors pa 'next' pointers of oldNode[0] ( in case there's a merge and oldNode[1].level > oldNode[0].level oldNode[1]'s pa will point to n as well )
	            //to the new node n.
	            //and unmark pa "pointers"
	            for(i = 0; i < n.level; i++)
	            {   
	            	 pa[i].setNext(i, n);
	            	 pa[i].Marks[i] =false;
	            }
				// Node n is fully linked to the list, set it as live.
	            n.live = true;
	            
	            if(merge[j])
	            {
	            	oldNode[1].trie=null;
	            }
	            oldNode[0].trie=null;
	        }
	        else
	        {
	            n.trie=null;
	        }    
	}
	
	
}
