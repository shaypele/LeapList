package utils;

public class TrieVal {
	short val;
	TrieVal [] child = null;
	
	public TrieVal (int size, short val){
		this.val = val;
	
		if (size != 0) {
			child = new TrieVal[size];
		
			for (int i = 0; i < size; i++){
				child[i] = new TrieVal(0, (short)0);
			}
		}
	}
}
