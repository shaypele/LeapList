#pragma once

#include <vector>
#include "cpp_framework.h"
#include "LeapSet.h"
#include "Trie.h"



static const char MAX_LEVEL = 10;

static const int NODE_SIZE = 60;

class LeapNode
{
public:

	


	volatile boolean live ;
	volatile long low;
	volatile  long high;
	volatile  int count;
	volatile char level;
	 LeapSet** data ; //the array must be sorted by keys so that LeapSet* with the smallest key is at LeapSet*[0] etc.
	 std::vector<CCP::AtomicReference<LeapNode>*> next ;
	 Trie* trie;
	volatile  boolean Marked;
	 int lastID ;
	 CCP::ReentrantLock nodeLock;
	


	LeapNode(void);
	LeapNode (boolean live, long low, long high, int count, char level)  ;
	~LeapNode(void);

	void init();
	boolean tryLock();
	void lock();
	void unlock();
	LeapNode* getNext (int i);
	void	setNext(int level, LeapNode* node);
};
