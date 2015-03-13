package leapListReg;
import utils.LeapSet;
import utils.Trie;
/*
 * LeapNode class represent the node in the LeapList structure,
 * each node has boolean live that is true if the node is in the list and false otherwise,
 * long low that represent the lowest key that can be in the node,
 * long high that represent the highest key that can be in the node,
 * int count that is the number of key value pairs in the node,
 * byte level that is the number of levels that the node has,
 * LeapSet[] data for the key value pairs
 * LeapNode[] next that keeps the next node in each level,
 * trie for finding the values in the node.
 */

public class LeapNode {
	boolean live = true;
	public long low;
	public long high;
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
		
		if (sortedPairs != null){
			this.data= sortedPairs;
			trie = new Trie(sortedPairs, sortedPairs.length);
		}
			
	}
	
	public LeapNode (){
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
}
