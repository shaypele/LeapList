#pragma once
#include "LeapList.h"

class LeapListDB
{
public:

	 LeapList** LeapLists ;

	LeapListDB(void);
	~LeapListDB(void);

	 LeapList*  GetListByIndex (int index);
	void* lookUp (LeapList* l, long key);
	void leapListUpdate (LeapList* * ll, long * keys, void* * values, int size);
	char getLevel();
	void updateSetup (  LeapList** ll, long * keys, void* * values, int size, 
										LeapNode** n, LeapNode* ** newNode, int * maxHeight, boolean* split, boolean* changed, int i);

	boolean insert (LeapNode** newNode, LeapNode* n, long key, void* val, boolean split);

	void  updateRelease (int size, LeapNode*** pa, LeapNode*** na, LeapNode** n, LeapNode*** newNode, boolean* split,
									boolean* changed, int j);

	void leapListRemove(LeapList** ll, long* keys, int size);

	 void RemoveReleaseAndUpdate(int size, LeapNode*** pa,
			LeapNode*** na, LeapNode** n, LeapNode*** oldNode,
			boolean* merge, boolean* changed, int j,LeapNode*** pa_Node1,LeapNode*** na_Node1);

	 void* find(LeapNode* node,long key);

		boolean remove(LeapNode** old_node, LeapNode* n,
				 long k, boolean merge);

	 void RemoveSetup(	LeapList** ll, long* keys,int size,
						LeapNode** n, LeapNode*** oldNode,
						boolean* merge, boolean* changed, int j);

	 void** RangeQuery (LeapList* l,long low, long high);

};
