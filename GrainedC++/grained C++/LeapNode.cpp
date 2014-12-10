#include "LeapNode.h"
#include <vector>
#include "cpp_framework.h"

LeapNode::LeapNode(void)
{
}

volatile boolean live = true;
	volatile long low;
	volatile  long high;
	volatile  int count;
	volatile char level;
	 LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	 vector<AtomicReference<LeapNode>> next ;
	Trie* trie;
	volatile  boolean Marked;
	 int lastID = 0;
	
	 final ReentrantLock nodeLock = new ReentrantLock();
	
	 LeapNode (boolean live, long low, long high, int count, char level, LeapSet[] sortedPairs) {
		this();
		this->live = live;
		this->level = level;
		this->low = low;
		this->high = high;
		this->count = count;
		this->level = level;
		
		
		if (sortedPairs != null)
			trie = new Trie(sortedPairs, sortedPairs->length);
	}
	
	 LeapNode (){
		next=new  ArrayList<AtomicReference<LeapNode>>(LeapList->MAX_LEVEL);
		for (int i = 0 ; i <LeapList->MAX_LEVEL ; i ++ ){
			next.add(i, new AtomicReference<LeapNode>());
		}
			
		this->Marked = false;
		this->live = false; 
		for (int i = 0 ; i < LeapList->NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	 boolean tryLock(){
		return nodeLock.tryLock();
	}
	
	 void lock(){
		nodeLock.lock();
	}
	
	 void unlock(){
		
		//if (nodeLock.isHeldByCurrentThread())
			nodeLock.unlock();
	}
	
	 LeapNode* getNext (int i){
		return next.get(i).get();
	}
	
	 void setNext(int level, LeapNode* node){
		next.get(level).set(node);
	}


LeapNode::~LeapNode(void)
{
}
