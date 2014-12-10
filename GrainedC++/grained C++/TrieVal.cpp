#include "TrieVal.h"
TrieVal::TrieVal ( void ){
	this->val = 0;
}
TrieVal::TrieVal (int size, short val){
		this->val = val;
	
		if (size != 0) {
			this->child = new TrieVal*[size];
		
			for (int i = 0; i < size; i++){
				this->child[i] = new TrieVal(0, (short)0);
			}
		}
	}



TrieVal::~TrieVal(void)
{
}
