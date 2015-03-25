package leapListReg;

import java.util.ArrayList;

import utils.Trie;
/*
 * the LeapList class represent the list of nodes,
 * it has LeapNode head that is the sentinel head node for the list.
 */

public class LeapList {
	static final byte MAX_LEVEL = 10;
	static final int NODE_SIZE = 300;
	
	LeapNode head;
	LeapNode tail;

	public LeapList () {
		head = new LeapNode (true, Long.MIN_VALUE, Long.MIN_VALUE, 0, MAX_LEVEL, null);
		tail = new LeapNode (true, Long.MAX_VALUE , Long.MAX_VALUE, 0, MAX_LEVEL, null);
		for (int i = 0; i < MAX_LEVEL; i++){
			head.setNext( i, tail);
		}
	}
	//returns the head node of the list
	public LeapNode GetHeadNode(){
		return this.head;
	}
 	
	/*
	 * The method receives a key and two Node arrays and returns the Node with the range that matches the key.
	 * On the way it fills the predecessor and successor arrays.
	 */
	LeapNode searchPredecessor ( long key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode x, x_next = null;
		boolean restartSearch = false;
		do{
			x = this.head;
			restartSearch = false;
			// Go over all levels, top to bottom to find all predecessors and their successors of the node that might contain key.
			for (int i = MAX_LEVEL -1; i >= 0; i--) {
				while (true){
					x_next = x.getNext(i);
					// If next node isn't live anymore then restart search.
					if (!x_next.live ){
						restartSearch = true;
						break;
					}
					// Found upper bound, proceed to next level
					if (x_next.high >= key)
						break;
					else
						x = x_next;
				}
				if (restartSearch){
					break;
				}
				
				if (pa != null)
					pa[i] = x;
				if (na != null)
					na[i] = x_next;
			}
		}while(restartSearch);
		return x_next;
	}
	
	/*
	 * The function receives a key and returns the value object matching that key or null if it does not exist.
	 * the implementation is lock free.
	 */
	public Object lookUp (long key){
		int index ;
		Object retVal = null;
		LeapNode [] na = new LeapNode[MAX_LEVEL];
		LeapNode [] pa = new LeapNode[MAX_LEVEL];
		key+= 2; // avoid sentinel 
		// Used here just to locate the node that k is supposed to be in.
		LeapNode ret = searchPredecessor( key, pa, na);
		//if trie is used use trieFindVal to find the key index
        if (Trie.USE_TRIE){
			try
			{
				index = ret.trie.trieFindVal(key);
			}
			catch(NullPointerException e)
			{
				return null;
			}
        }
        else{// If Trie isn't used, go over all elements in the data array of n in order to find the given key.

        	index = ret.findIndex(key);
        }
		
		if (index != -1)
		{//if found get the value from the index
			retVal =  ret.data[index].value;
		}
		return retVal;
	}
	
	/*
	 * The function receives a low and high keys and returns an array of objects matching the keys.
	 * the implementation is lock free.
	 */
	public Object[] RangeQuery (long low, long high){
	    LeapNode n;
	    ArrayList<Object> rangeSet = new ArrayList<Object>(); 
	    ArrayList<LeapNode> nodesToIterate = new ArrayList<LeapNode>();
	    boolean restartSearch = false;
	    low = low+2; // Avoid sentinel
	    high = high+2; // Avoid sentinel
	    // First get a set of sequential nodes that all of them are live. It gives us a snapshot of the strucutre.
	    // Then iterate them while "offline" and return the set.
	    do{
	    	restartSearch = false;
	    	nodesToIterate.clear();
			// Used here just to locate the node that low is supposed to be in.
	    	n = searchPredecessor( low, null, null);
	    	//traverse the nodes from n and add to nodesToIterate list all the node that their high value is less then the given high.
	    	nodesToIterate.add(n);
	    	while (high>n.high)
	 	    {
	    		 if (!n.live){// if the node is not live then restart the outer loop
	    			 restartSearch = true;
	    			 break;
	    		 }
	    		 if (n.getNext(0) != null){
	 	    		n = n.getNext(0);
	 	    		nodesToIterate.add(n);
	 	    	}
	 	    }
	    } while(restartSearch);
	    
	    //traverse the snapshot of the list of nodes nodesToIterate and add all the values from them that in the range.
	    for (LeapNode node : nodesToIterate){
	    	addValuesToSet(low, high, node, rangeSet);
	    }
	   
	    return rangeSet.toArray();
	}

	/*
	 * The method receives a low and high value, a Node and an Object arrayList, looks up the keys in the Node and adds
	 * the corresponding Objects to the arrayList.
	 */
	void addValuesToSet(long low, long high, LeapNode n,
			ArrayList<Object> rangeSet) {
		for (int i = 0; i < n.count ; i++)
		{
			if (n.data[i].key >= low && n.data[i].key <= high )
			{
				rangeSet.add(n.data[i].value);
			}
		}
	}

}
