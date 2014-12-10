#pragma once
#include <vector>
#include "cpp_framework.h"
#include "LeapNode.h"
#include "LeapSet.h"


class LeapList
{
public:


	LeapNode* head;
	LeapNode* tail;

	LeapList(void);
	~LeapList(void);

	LeapNode* GetHeadNode();

	LeapNode* searchPredecessor ( long key, LeapNode** pa, LeapNode** na);

	void* lookUp (long key);

	 void** RangeQuery (long low, long high);
private:
	void addValuesToSet(long low, long high, LeapNode* n, std::vector<void*>* rangeSet);
};
