package leapListReg;

import java.util.ArrayList;

import utils.LeapSet;


public class LeapList {
	static final byte MAX_LEVEL = 10;
	
	static final int NODE_SIZE = 300;
	
	LeapNode head;

	public LeapList () {
		head = new LeapNode (true, Long.MIN_VALUE, Long.MIN_VALUE, 0, MAX_LEVEL, null);
		LeapNode tail = new LeapNode (true, Long.MAX_VALUE, Long.MAX_VALUE, 0, MAX_LEVEL, null);
		for (int i = 0; i < MAX_LEVEL; i++){
			head.next[i] = tail;
		}
	}
	
 	
	
	LeapNode searchPredecessor ( long key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode x, x_next;
		
		x = this.head;
		
		for (int i = MAX_LEVEL -1; i >= 0; i--) {
			while (true){
				x_next = x.next[i];
				if (x_next.high > key)
					break;
				else
					x = x_next;
			}
			if (pa != null)
				pa[i] = x;
			if (na != null)
				na[i] = x_next;
		}
		
		return na[0];
	}
	
	Object lookUp (long key){
		LeapNode [] na = new LeapNode[MAX_LEVEL];
		LeapNode [] pa = new LeapNode[MAX_LEVEL];
		LeapNode ret = searchPredecessor( key, pa, na);
		return ret.data[ret.trie.trieFindVal(key)].value;
	}

	Object[] RangeQuery (long low, long high){
	    LeapNode n;
	    ArrayList<Object> rangeSet = new ArrayList<Object>(); 
	    low = low+2; // Avoid sentinel
	    high = high+2;
	
	    n = searchPredecessor( low, null, null);
	    
	    do
	    {
	    	n = addValuesToSet(low, high, n, rangeSet);
	    }while (high>n.high);
	    addValuesToSet(low, high, n, rangeSet);
	   
	    return rangeSet.toArray();
	}



	LeapNode addValuesToSet(long low, long high, LeapNode n,
			ArrayList<Object> rangeSet) {
		for (int i = 0; i < n.count ; i++)
		{
			if (n.data[i].key >= low && n.data[i].key <= high )
			{
				rangeSet.add(n.data[i].value);
			}
		}
		n = n.next[0].UnMark();
		return n;
	}

}
