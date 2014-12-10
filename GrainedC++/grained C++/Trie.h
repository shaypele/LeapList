#include "TrieVal.h"
#include "TrieMetadata.h"
#include "LeapSet.h"
#pragma once

class Trie
{


public:

		 int nodeNum ;
	TrieVal* head;
	TrieMetadata* meta;

	 Trie (long key, short value);
	 Trie (LeapSet** data ,int size);
	 void createOneNodeTrie(long key, short value);
	 void computeTriePrefix (long low, long high);
	 short trieFindVal (long key);
	 int trieNodesNum();


		~Trie(void);
private:
	 int recTrieNodesNum(TrieVal* curr, char curLevel);


		
		
		


};
