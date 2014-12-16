package leapListReg;

import java.util.ArrayList;


public class LeapList {
	static final byte MAX_LEVEL = 10;
	
	static final int NODE_SIZE = 60;
	
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
		
		for (int i = MAX_LEVEL -1; i >= 0; i--) {
			while (true){
				x_next = x.next[i];
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
		LeapNode ret = searchPredecessor( key, pa, na);
		index = ret.trie.trieFindVal(key);
		if (index != -1)
		{
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
	
	    n = searchPredecessor( low, null, null);
	    
	    while (high>n.high)
	    {
	    	n = addValuesToSet(low, high, n, rangeSet);
	    	if (n.next[0] != null){
	    		n = n.next[0];
	    	}
	    }
	    addValuesToSet(low, high, n, rangeSet);
	   
	    return rangeSet.toArray();
	}



	/*
	 * The method receives a low and high value, a Node and an Object arrayList, looks up the keys in the Node and adds
	 * the corresponding Objects to the arrayList.
	 */
	private LeapNode addValuesToSet(long low, long high, LeapNode n,
			ArrayList<Object> rangeSet) {
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
