#pragma once

class TrieMetadata
{
public:
	TrieMetadata (char prefix_length, long prefix);
	TrieMetadata ();
	char prefix_length ;
	long prefix ;
	~TrieMetadata(void);
};
