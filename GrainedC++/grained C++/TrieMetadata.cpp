#include "TrieMetadata.h"
	
TrieMetadata::TrieMetadata (){};

	TrieMetadata::TrieMetadata (char prefix_length, long prefix){
		this->prefix = prefix; 
		this->prefix_length = prefix_length;
	}

TrieMetadata::~TrieMetadata(void)
{
}
