package leapListReg;

import utils.LeapSet;
import utils.Trie;

public class LeapNode {
	boolean live = true;
	long low;
	long high;
	public int count;
	byte level;
	public LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	public LeapNode [] next = new LeapNode [LeapList.MAX_LEVEL];
	Trie trie;
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs) {
		this();
		this.level = level;
		this.low = low;
		this.high = high;
		this.count = count;
		this.level = level;
		
		if (sortedPairs != null)
			this.low=low;
			this.high=high;
			this.count =count;
			this.level=level;
			this.data= sortedPairs;
			
			if(sortedPairs!=null)
			trie = new Trie(sortedPairs, sortedPairs.length);
	}
	
	public LeapNode (){
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	public LeapNode UnMark() {
		// TODO see how to handle with transactions.
		return this;
	}

}
