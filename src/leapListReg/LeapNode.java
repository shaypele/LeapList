package leapListReg;

import utils.LeapSet;
import utils.Trie;

public class LeapNode {
	boolean live = true;
	double low;
	double high;
	int count;
	int level;
	LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	LeapNode [] next = new LeapNode [LeapList.MAX_LEVEL];
	Trie trie;
	
	public LeapNode (boolean live, double low, double high, int count, int level, LeapSet[] sortedPairs){
		if (sortedPairs != null)
			trie = new Trie(sortedPairs, sortedPairs.length);
	}
	
	public LeapNode (){
	}

}
