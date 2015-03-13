package leapListReg;

import java.util.ArrayList;

import org.deuce.Atomic;

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
		tail = new LeapNode (true, Long.MAX_VALUE, Long.MAX_VALUE, 0, MAX_LEVEL, null);
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
	 * the attribute @Atomic gives us the use of transactional memory in software from the library DeuceStm
	 * and make the function run atomically or not run at all if it fails it tries again until it succeed.
	 */
	@Atomic()
	LeapNode searchPredecessor ( long key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode x, x_next = null,xPred = null;
		
		boolean restartLook = false;
		do{
		x = head;
		xPred = head;
		restartLook = false;
		// Go over all levels, top to bottom to find all predecessors and their successors of the node that might contain key.
		for (int i = MAX_LEVEL -1; i >= 0; i--) {
			while (true){
				x_next = x.getNext(i);
				if (!x_next.live || x.Marks[i] ){
					restartLook = true;
					break;
				}
				// Found upper bound, proceed to next level
				if (x_next.high >= key)
					break;
				else
				{
					xPred  = x;
					x = x_next;
				}
			}
			// If x or xPred next node i pointer is marked then restart search.
		  if (xPred.Marks[i] ||  x.Marks[i] )
			  restartLook = true;
			if(restartLook == true){
				break;
			}
			if (pa != null)
				pa[i] = x;
			if (na != null)
				na[i] = x_next;
			
		}
		}while(restartLook);
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
		try
		{
			//use trie trieFindVal function to find the key index
            index = ret.trie.trieFindVal(key);
            if (index != -1)
            {//if found get the value from the index
                return ret.data[index].value;
            }
		}
		catch(NullPointerException e)
		{
			return null;
		}
		return retVal;
	}

	/*
	 * The function receives a low and high keys and returns an array of objects matching the keys.
	 */
	public Object[] RangeQuery (long low, long high){
		LeapNode n = new LeapNode();
	    ArrayList<Object> rangeSet = new ArrayList<Object>(); 
	    ArrayList<LeapNode> nodesToIterate = new ArrayList<LeapNode>();
	    low = low+2; // Avoid sentinel
	    high = high+2; // Avoid sentinel
	 
	    getAndAddSucssesor(nodesToIterate,n,low,high);
	    //traverse the snapshot of the list of nodes nodesToIterate and add all the values from them that in the range.
	    for (LeapNode node : nodesToIterate){
	    	addValuesToSet(low, high, node, rangeSet);
	    }
	   
	    return rangeSet.toArray();
	}
	
	/*
	 * the function run atomically and traverse the nodes and add the to the list nodesToIterate
	 * if the highest key in the node is less then the given high.
	 * the function use the Atomic attribute to run atomically or not run at all until it succeed.
	 */
	@Atomic()
	private void getAndAddSucssesor(ArrayList<LeapNode> nodesToIterate, LeapNode n,
			long low,long high) {
		nodesToIterate.clear();
		// Used here just to locate the node that low is supposed to be in.
		n = searchPredecessor( low, null, null);
    	//traverse the nodes from n and add to nodesToIterate list all the node that their high value is less then the given high.
	    nodesToIterate.add(n);	
	    while (high>n.high)
	    {
	    	if (!n.live)
	    	{
    			 break;
    		}
	    	if (n.getNext(0) != null)
	    	{
 	    		n = n.getNext(0);
 	    		nodesToIterate.add(n);
 	    	}
	    }
	    
	}

	/*
	 * The method receives a low and high value, a Node and an Object arrayList, looks up the keys in the Node and adds
	 * the corresponding Objects to the arrayList.
	 */
	LeapNode addValuesToSet(long low, long high, LeapNode n,
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
