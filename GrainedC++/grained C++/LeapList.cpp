#include <climits> 
#include "LeapList.h"
#include "LeapNode.h"
#include "LeapSet.h"




static final char MAX_LEVEL = 10;

static final int NODE_SIZE = 60;

LeapNode*head;
LeapNode*tail;

LeapList::LeapList(void)
{
	head = new LeapNode (true, LONG_MIN,   LONG_MIN, 0, MAX_LEVEL, null);
	tail = new LeapNode (true, LONG_MAX	 , LONG_MAX, 0, MAX_LEVEL, null);
	for (int i = 0; i < MAX_LEVEL; i++){
		head->setNext( i, tail);
	}
}

LeapList::~LeapList(void)
{
}
	
	 LeapNode* GetHeadNode(){
		return this.head;
	}
 	
	
	LeapNode* searchPredecessor ( long key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode* x, x_next = null;
		boolean restartSearch = false;
		do{
			x = this.head;
			restartSearch = false;
			for (int i = MAX_LEVEL -1; i >= 0; i--) {
				while (true){
					x_next = x.getNext(i);
					if (!x_next.live ){
						restartSearch = true;
						break;
					}
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
	
	// TODO: check if marked/live.
	 Object lookUp (long key){
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
	
	// TODO: check if marked/live.
	 Object[] RangeQuery (long low, long high){
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
	    	n = searchPredecessor( low, null, null);
	    	nodesToIterate.add(n);
	    	while (high>n.high)
	 	    {
	    		 if (!n.live){
	    			 restartSearch = true;
	    			 break;
	    		 }
	    		 if (n.getNext(0) != null){
	 	    		n = n.getNext(0);
	 	    		nodesToIterate.add(n);
	 	    	}
	 	    }
	    } while(restartSearch);
	    
	    for (LeapNode node : nodesToIterate){
	    	addValuesToSet(low, high, node, rangeSet);
	    }
	   
	    return rangeSet.toArray();
	}



	void addValuesToSet(long low, long high, LeapNode* n,
			ArrayList<Object> rangeSet) {
		for (int i = 0; i < n.count ; i++)
		{
			if (n.data[i].key >= low && n.data[i].key <= high )
			{
				rangeSet.add(n.data[i].value);
			}
		}
	}


