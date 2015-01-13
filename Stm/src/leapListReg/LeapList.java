package leapListReg;

import java.util.ArrayList;

import org.deuce.Atomic;



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
	
	public LeapNode GetHeadNode(){
		return this.head;
	}
 	
	@Atomic()
	LeapNode searchPredecessor ( long key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode x, x_next = null;
		boolean   xRef = false; 
		boolean restartLook = false;
		do{
			x = head;
			xRef = false; 
			restartLook = false;
			for (int i = MAX_LEVEL -1; i >= 0; i--)
			{
				while (true)
				{
					x_next = x.getNext(i);
					if (x_next.high >= key)
						break;
					else
					{
						x = x_next;
						xRef = x.Marks[i];
					}
				}
			  if (xRef ||  x.Marks[i])
			  {
				  restartLook = true;
			  }
			  if(restartLook == true)
					break;
			  
				if (pa != null)
				{
					pa[i] = x;
				}
				if (na != null)
				{
					na[i] = x_next;
				}
			}
		}while(restartLook);
		return x_next;
	}
	
	public Object lookUp (long key){
		int index ;
		Object retVal = null;
		LeapNode [] na = new LeapNode[MAX_LEVEL];
		LeapNode [] pa = new LeapNode[MAX_LEVEL];
		key+= 2; // avoid sentinel 
		LeapNode ret = searchPredecessor( key, pa, na);
		try
		{
            index = ret.trie.trieFindVal(key);
            if (index != -1)
            {
                return ret.data[index].value;
            }
		}
		catch(NullPointerException e)
		{
			return null;
		}
		return retVal;
	}

	public Object[] RangeQuery (long low, long high){
		LeapNode n = new LeapNode();
	    ArrayList<Object> rangeSet = new ArrayList<Object>(); 
	    ArrayList<LeapNode> nodesToIterate = new ArrayList<LeapNode>();
	    low = low+2; // Avoid sentinel
	    high = high+2; // Avoid sentinel
	 
	    getAndAddSucssesor(nodesToIterate,n,low,high);
	    
	    for (LeapNode node : nodesToIterate){
	    	addValuesToSet(low, high, node, rangeSet);
	    }
	   
	    return rangeSet.toArray();
	}

	@Atomic()
	private void getAndAddSucssesor(ArrayList<LeapNode> nodesToIterate, LeapNode n,
			long low,long high) {
		nodesToIterate.clear();
		n = searchPredecessor( low, null, null);
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
