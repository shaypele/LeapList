#include "LeapNode.h"
#include <vector>
#include "cpp_framework.h"

LeapNode::LeapNode (boolean live, long low, long high, int count, char level) {
		this->init();
		this->live = live;
		this->level = level;
		this->low = low;
		this->high = high;
		this->count = count;
		this->level = level;
	}

void LeapNode::init(){
this->lastID = 0;
		 data = new LeapSet*[NODE_SIZE];
		for (int i = 0 ; i <MAX_LEVEL ; i ++ ){
			next.push_back( new CCP::AtomicReference<LeapNode>());
		}
			
		this->Marked = false;
		this->live = false; 
		for (int i = 0 ; i < NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
}
	
	 LeapNode::LeapNode (){
		 init();
	}
	
	 boolean LeapNode::tryLock(){
		return nodeLock.tryLock();
	}
	
	 void LeapNode::lock(){
		nodeLock.lock();
	}
	
	 void LeapNode::unlock(){
		
		//if (nodeLock.isHeldByCurrentThread())
			nodeLock.unlock();
	}
	
	 LeapNode* LeapNode::getNext (int i){
		return next.at(i)->get();
	}
	
	 void LeapNode::setNext(int level, LeapNode* node){
		next.at(level)->set(node);
	}


LeapNode::~LeapNode(void)
{
}
