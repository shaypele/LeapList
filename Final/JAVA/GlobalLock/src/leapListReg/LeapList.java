package leapListReg;

import java.util.ArrayList;
/*
 * the LeapList class represent the list of nodes,
 * it has LeapNode head that is the sentinel head node for the list.
 */

public class LeapList {
	static final byte MAX_LEVEL = 10;
	
	static final int NODE_SIZE = 300;
	
	private LeapNode head;

	public LeapList () {
		head = new LeapNode (true, Long.MIN_VALUE, Long.MIN_VALUE, 0, MAX_LEVEL, null);
		LeapNode tail = new LeapNode (true, Long.MAX_VALUE, Long.MAX_VALUE, 0, MAX_LEVEL, null);
		for (int i = 0; i < MAX_LEVEL; i++){
			head.next[i] = tail;
		}
	}
	
	public LeapNode GetHeadNode(){
		return this.head;
	}
 	
	/*
	 * The method receives a key and two Node arrays and returns the Node with the range that matches the key.
	 * On the way it fills the predecessor and successor arrays.
	 */
	public LeapNode searchPredecessor ( long key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode x, x_next = null;
		
		x = this.head;
		// Go over all levels, top to bottom to find all predecessors and their successors of the node that might contain key.
		for (int i = MAX_LEVEL -1; i >= 0; i--) {
			while (true){
				x_next = x.next[i];
				// Found upper bound, proceed to next level
				if (x_next.high >= key)
					break;
				else
					x = x_next;
			}
			if (pa != null)
				pa[i] = x;
			if (na != null)
				na[i] = x_next;
		}
		
		return x_next;
	}
	
	/*
	 * The function receives a key and returns the value object matching that key or null if it does not exist.
	 */
	public Object lookUp (long key){
		int index ;
		Object retVal = null;
		LeapNode [] na = new LeapNode[MAX_LEVEL];
		LeapNode [] pa = new LeapNode[MAX_LEVEL];
		key+= 2; // avoid sentinel 
		// Used here just to locate the node that k is supposed to be in.
		LeapNode ret = searchPredecessor( key, pa, na);
		// Call trie.trieFindVal to find the key k index in the node. it return -1 if not found
		index = ret.trie.trieFindVal(key);
		if (index != -1)
		{//if found get the value from the index
			retVal =  ret.data[index].value;
		}
		return retVal;
	}

	/*
	 * The function receives a low and high keys and returns an array of objects matching the keys.
	 */
	public Object[] RangeQuery (long low, long high){
	    LeapNode n;
	    ArrayList<Object> rangeSet = new ArrayList<Object>(); 
	    low = low+2; // Avoid sentinel
	    high = high+2; // Avoid sentinel
	    
		// Used here just to locate the node that low is supposed to be in.
	    n = searchPredecessor( low, null, null);
	    
		// Traverse the list from the node that could contain low to the last node that could contain high.
	    //and add all the values from the nodes in the range to the set.
	    while (high>n.high)
	    {
	    	n = addValuesToSet(low, high, n, rangeSet);
	    	if (n.next[0] != null){
	    		n = n.next[0];
	    	}
	    }
	    //add the values that in the range to the set from the last node.
	    addValuesToSet(low, high, n, rangeSet);
	   
	    return rangeSet.toArray();
	}

	/*
	 * The method receives a low and high value, a Node and an Object arrayList, looks up the keys in the Node and adds
	 * the corresponding Objects to the arrayList.
	 */
	private LeapNode addValuesToSet(long low, long high, LeapNode n,
			ArrayList<Object> rangeSet) {
		//run over all the keys and add to rangeSet the values that matches the keys in the range.
		for (int i = 0; i < n.count ; i++)
		{
			if (n.data[i].key >= low && n.data[i].key <= high )
			{
				rangeSet.add(n.data[i].value);
			}
		}
		return n;
	}

}
