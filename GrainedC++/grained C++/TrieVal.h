#pragma once

class TrieVal
{
public:
	TrieVal ( void );
	TrieVal(int size, short val);
	short val;
	TrieVal** child ;
	~TrieVal(void);
};
