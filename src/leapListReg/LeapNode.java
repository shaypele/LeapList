package leapListReg;

import utils.LeapSet;
import utils.Trie;

public class LeapNode {
	boolean live = true;
	long low;
	long high;
	int count;
	byte level;
	LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	LeapNode [] next = new LeapNode [LeapList.MAX_LEVEL];
	Trie trie;
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs){
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
	}
	
	public LeapNode UnMark() {
		// TODO see how to handle with transactions.
		return this;
	}

}
