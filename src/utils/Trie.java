package utils;

public class Trie {

	final int TRIE_KEY_MAX_DIGITS =  16;
	final int LAST_DIGIT_MASK =0xf;
    final int LAST_2_DIGITS_MASK = 0xff;
	public int nodeNum = 1;
	
	private TrieMetadata meta;
	private TrieVal head;
	
	public Trie (int key, short value){
		
		TrieVal val = new TrieVal(16, (short)0);
		head = val;
		
		this.meta = new TrieMetadata(TRIE_KEY_MAX_DIGITS - 1, (key>>4));
		head.child[(key & 16)].val = (short) (value  + 1);
		
	}
	
	public Trie (LeapSet[] data ,int size){
		short counter = 1;
		
		this.meta = new TrieMetadata();
		
		computeTriePrefix(data[0].key, data[size - 1].key);
		
		if (meta.prefix_length == 14){
			this.head = new TrieVal(256, (short)0);
			
			for (int i = 0; i < size; i++){
				int index = (data[i].key & LAST_2_DIGITS_MASK);
				this.head.child[index].val = (short)counter;
				counter ++;
			}
		}
		
		else if (meta.prefix_length == 15){
			this.head = new TrieVal (16, (short)0);
			
			for (int i = 0;i < size; i++){
				int index = (data[i].key & LAST_DIGIT_MASK);
				this.head.child[index].val = counter;
				counter ++;
			}
		}
		
		else {
			if (this.meta.prefix_length % 2 == 0){
				this.head = new TrieVal(256, (short)0);
			}
			else {
				this.head = new TrieVal(16, (short)0);
			}
			
			for (int i =0; i < size; i++){
				int shiftDigits = (TRIE_KEY_MAX_DIGITS - meta.prefix_length);
				int index;
				TrieVal curr = this.head;
				int currKey = data[i].key;
				
				if (shiftDigits % 2 == 0){
					index = (currKey >> ((shiftDigits - 2) << 2)) & LAST_2_DIGITS_MASK;
				}
				else{
					index = (currKey >> ((shiftDigits - 1) << 2)) & LAST_DIGIT_MASK;
					shiftDigits++;
				}
				
				while (shiftDigits > 2){
					
					if (curr.child[index].child == null){
							curr.child[index] = new TrieVal(256, curr.child[index].val);
							nodeNum++;
					}
					
					curr = curr.child[index];
					shiftDigits = shiftDigits -2;
					index = (currKey >> ((shiftDigits -2) <<2)) & LAST_2_DIGITS_MASK;
				}
				
				curr.child[index].val = counter;
				counter++;
			}
		}
		
	}
	
	void computeTriePrefix (int low, int high){
		
		for (int i = 1; i <= TRIE_KEY_MAX_DIGITS; i++){	
			if((low >> (i << 2)) == (high >> (i << 2))){
				this.meta.prefix_length = TRIE_KEY_MAX_DIGITS - i;
				this.meta.prefix = (low >> (i << 2));
				return;
			}
		}
	}
	
	public short trieFindVal (int key){
		int shiftDigits = (TRIE_KEY_MAX_DIGITS - meta.prefix_length);
		int index;
		TrieVal curr = head;
		
		if (head == null)
			return -1;
		
		if ((key >> (shiftDigits << 2)) != meta.prefix)
			return -1;
		
		if (shiftDigits % 2 == 0){
			index = (key >> ((shiftDigits - 2) << 2)) & LAST_2_DIGITS_MASK;
		}
		else{
			index = (key >> ((shiftDigits - 1) << 2)) & LAST_DIGIT_MASK;
			shiftDigits ++;
		}
		
		while (shiftDigits > 2){
			if (curr.child[index].child == null)
				return -1;
			
			curr = curr.child[index];
			shiftDigits = shiftDigits - 2;
			index = (key >> ((shiftDigits - 2) << 2)) & LAST_2_DIGITS_MASK;
		}
		
		if (curr.child[index].val == 0)
			return -1;
		
		return (short) (curr.child[index].val - 1);
	}

	private int recTrieNodesNum(TrieVal curr, int curLevel){
		if (curLevel == 7){
			return 1;
		}
		else{
			TrieVal real  = curr;
			int sum = 0;
			for (int i = 0; i < 256; i++){
				if (real.child[i].child != null)
					sum = sum + recTrieNodesNum(real.child[i], curLevel + 1);
			}
			sum = sum + 1;
			return sum;
		}
	}
	
	public int trieNodesNum(){
		if (head == null)
			return 0;
		
		if (meta.prefix_length >= 14)
			return 1;
		
		int element_sum = ((meta.prefix_length %2) == 0)? 256: 16;
		TrieVal realHead = head;
		int sum = 0; 
		for (int i = 0; i < element_sum; i ++){
			if (realHead.child[i].child != null)
				sum = sum + recTrieNodesNum(realHead.child[i], (meta.prefix_length / 2) + 1);
		}
		sum  = sum + 1;
		return sum;
	}

	//public void printTrie(){} TODO
}
