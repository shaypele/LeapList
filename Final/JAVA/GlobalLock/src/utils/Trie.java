package utils;

public class Trie {

	final int TRIE_KEY_MAX_DIGITS =  16;
	final int LAST_DIGIT_MASK =0xf;
    final int LAST_2_DIGITS_MASK = 0xff;
	public int nodeNum = 1;
	
	private TrieMetadata meta;
	private TrieVal head;
	
	/* Creates a new trie with the given (key, value) pair */
	private void createOneNodeTrie(long key, short value){
		/* Creates a trie with only leaves */
		TrieVal val = new TrieVal(16, (short)0);
		head = val;
		
		this.meta = new TrieMetadata((byte) (TRIE_KEY_MAX_DIGITS - 1), (key>>4));
		head.child[(int) (key & LAST_DIGIT_MASK)].val = (short) (value  + 1);
		
	} 
	
	public Trie (long key, short value){
		
		createOneNodeTrie( key,  value);
	}
	

/* Creates a new trie which is based on the given array.*/
	public Trie (LeapSet[] data ,int size){
		short counter = 1;
		
		//  if the array is empty. 
		if ( size == 0 ){
			createOneNodeTrie( 0, (short)  0);
			return;
		}
		
		this.meta = new TrieMetadata();
		/* First compute the prefix */
		computeTriePrefix(data[0].key, data[size - 1].key);
		
		/* In case the trie has only leaves */
		if (meta.prefix_length == 14){
			/* The trie nedds to be with only 256 leaves */
			this.head = new TrieVal(256, (short)0);
			
			for (int i = 0; i < size; i++){
				int index = (int) (data[i].key & LAST_2_DIGITS_MASK);
				this.head.child[index].val = (short)counter;
				counter ++;
			}
		}
		
		else if (meta.prefix_length == 15){
			/* The trie nedds to be with only 16 leaves */
			this.head = new TrieVal (16, (short)0);
			
			for (int i = 0;i < size; i++){
				int index = (int) (data[i].key & LAST_DIGIT_MASK);
				this.head.child[index].val = counter;
				counter ++;
			}
		}
		
		else {
			/* In case the trie has inner nodes */
			if (this.meta.prefix_length % 2 == 0){
				/* The trie needs to be with 256 inner node */
				this.head = new TrieVal(256, (short)0);
			}
			else {
				/* The trie needs to be with 16 inner node */
				this.head = new TrieVal(16, (short)0);
			}
			/* Add each key from the array (if the inner nodes are not there, create them - their size is 256) */
			for (int i =0; i < size; i++){
				int shiftDigits = (TRIE_KEY_MAX_DIGITS - meta.prefix_length);
				int index;
				TrieVal curr = this.head;
				long currKey = data[i].key;
				
				if (shiftDigits % 2 == 0){
					index = (int) ((currKey >> ((shiftDigits - 2) << 2)) & LAST_2_DIGITS_MASK);
				}
				else{
					index = (int) ((currKey >> ((shiftDigits - 1) << 2)) & LAST_DIGIT_MASK);
					shiftDigits++;/* make shift_digits an even number */
				}
				/* Traverse the inner nodes in the trie */
				while (shiftDigits > 2){
					
					if (curr.child[index].child == null){
							curr.child[index] = new TrieVal(256, curr.child[index].val);
							nodeNum++;
					}
					
					curr = curr.child[index];
					shiftDigits = shiftDigits -2;
					index = (int) ((currKey >> ((shiftDigits -2) <<2)) & LAST_2_DIGITS_MASK);
				}
				
				/* Add the key-val to a leaf */
				curr.child[index].val = counter;
				counter++;
			}
		}
		
	}
	

	void computeTriePrefix (long low, long high){
		
		for (int i = 1; i <= TRIE_KEY_MAX_DIGITS; i++){	
			if((low >> (i << 2)) == (high >> (i << 2))){
				this.meta.prefix_length = (byte) (TRIE_KEY_MAX_DIGITS - i);
				this.meta.prefix = (low >> (i << 2));
				return;
			}
		}
	}
	/* Finds a given key in the trie (returns -1 in case the key is not part of the trie) */
	public short trieFindVal (long key){
		int shiftDigits = (TRIE_KEY_MAX_DIGITS - meta.prefix_length);
		int index;
		TrieVal curr = head;
		/* If the trie is empty, return */
		if (head == null)
			return -1;
		
		/* Check the base case (if the key does not match the prefix) */
		if ((key >> (shiftDigits << 2)) != meta.prefix)
			return -1;
		
		if (shiftDigits % 2 == 0){
			index = (int) ((key >> ((shiftDigits - 2) << 2)) & LAST_2_DIGITS_MASK);
		}
		else{
			index = (int) ((key >> ((shiftDigits - 1) << 2)) & LAST_DIGIT_MASK);
			shiftDigits ++;/* make shift_digits an even number */
		}
		
		/* Traverse the inner nodes in the trie */
		while (shiftDigits > 2){
			if (curr.child[index].child == null)
				return -1;
			
			curr = curr.child[index];
			shiftDigits = shiftDigits - 2;
			index = (int) ((key >> ((shiftDigits - 2) << 2)) & LAST_2_DIGITS_MASK);
		}
		/* Reached a leaf */
		
		if (curr.child[index].val == 0)
			return -1;
		
		return (short) (curr.child[index].val - 1);
	}

	private int recTrieNodesNum(TrieVal curr, byte curLevel){
		if (curLevel == 7){
			return 1;
		}
		else{
			TrieVal real  = curr;
			int sum = 0;
			for (int i = 0; i < 256; i++){
				if (real.child[i].child != null)
					sum = sum + recTrieNodesNum(real.child[i], (byte) (curLevel + 1));
			}
			sum = sum + 1;
			return sum;
		}
	}
	
	public int trieNodesNum(){
		/* If the trie is empty, return */
		if (head == null)
			return 0;
		
		if (meta.prefix_length >= 14)
			return 1;
		
		int element_sum = ((meta.prefix_length %2) == 0)? 256: 16;
		TrieVal realHead = head;
		int sum = 0; 
		for (int i = 0; i < element_sum; i ++){
			if (realHead.child[i].child != null)
				sum = sum + recTrieNodesNum(realHead.child[i], (byte) ((meta.prefix_length / 2) + 1));
		}
		sum  = sum + 1;
		return sum;
	}

}
