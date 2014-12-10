#include <climits> 
#include "LeapList.h"





LeapList::LeapList(void)
{
	head = new LeapNode (true, LONG_MIN,   LONG_MIN, 0, MAX_LEVEL);
	tail = new LeapNode (true, LONG_MAX	 , LONG_MAX, 0, MAX_LEVEL);
	for (int i = 0; i < MAX_LEVEL; i++){
		head->setNext( i, tail);
	}
}

LeapList::~LeapList(void)
{
}
	
LeapNode* LeapList::GetHeadNode(){
		return this->head;
	}
 	
	
	LeapNode* LeapList::searchPredecessor ( long key, LeapNode** pa, LeapNode** na){
		
		LeapNode* x, *x_next = 0 ;
		boolean restartSearch = false;
		do{
			x = this->head;
			restartSearch = false;
			for (int i = MAX_LEVEL -1; i >= 0; i--) {
				while (true){
					x_next = x->getNext(i);
					if (!x_next->live ){
						restartSearch = true;
						break;
					}
					if (x_next->high >= key)
						break;
					else
						x = x_next;
				}
				if (restartSearch){
					break;
				}
				
				if (pa != 0)
					pa[i] = x;
				if (na != 0)
					na[i] = x_next;
			}
		}while(restartSearch);
		return x_next;
	}
	
	// TODO: check if marked/live.
	 void* LeapList::lookUp (long key){
		int index ;
		void* retVal = 0;
		LeapNode** na = new LeapNode*[MAX_LEVEL];
		LeapNode** pa = new LeapNode*[MAX_LEVEL];
		key+= 2; // avoid sentinel 
		LeapNode* ret = searchPredecessor( key, pa, na);
		index = ret->trie->trieFindVal(key);
		if (index != -1)
		{
			retVal =  ret->data[index]->value;
		}
		return retVal;
	}
	
	// TODO: check if marked/live.
	 void** LeapList::RangeQuery (long low, long high){
	    LeapNode* n;
		int i ;
	    std::vector<void*> rangeSet ; 
	    std::vector<LeapNode*> nodesToIterate ;
	    boolean restartSearch = false;
	    low = low+2; // Avoid sentinel
	    high = high+2; // Avoid sentinel
	    // First get a set of sequential nodes that all of them are live. It gives us a snapshot of the strucutre.
	    // Then iterate them while "offline" and return the set.
	    do{
	    	restartSearch = false;
	    	nodesToIterate.clear();
	    	n = searchPredecessor( low, 0, 0);
			nodesToIterate.push_back(n);
	    	while (high>n->high)
	 	    {
	    		 if (!n->live){
	    			 restartSearch = true;
	    			 break;
	    		 }
	    		 if (n->getNext(0) != 0){
	 	    		n = n->getNext(0);
					nodesToIterate.push_back(n);
	 	    	}
	 	    }
	    } while(restartSearch);
	    
	    for ( i = 0 ; i < nodesToIterate.size(); i++){
			LeapNode* node = nodesToIterate[i];
	    	addValuesToSet(low, high, node, &rangeSet);
	    }
	   
	    return &rangeSet[0];
	}



	void LeapList::addValuesToSet(long low, long high, LeapNode* n,
			std::vector<void*>* rangeSet) {
		for (int i = 0; i < n->count ; i++)
		{
			if (n->data[i]->key >= low && n->data[i]->key <= high )
			{
				rangeSet->push_back(n->data[i]->value);
			}
		}
	}


