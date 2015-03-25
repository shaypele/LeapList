package leapListReg;
import utils.LeapSet;
import utils.Trie;
/*
 * LeapNode class represent the node in the LeapList structure,
 * each node has a volatile boolean live that is true if the node is in the list and false otherwise,
 * volatile long low that represent the lowest key that can be in the node,
 * volatile long high that represent the highest key that can be in the node,
 * volatile int count that is the number of key value pairs in the node,
 * volatile byte level that is the number of levels that the node has,
 * LeapSet[] data for the key value pairs
 * volatile boolean[] Marks - Marks[i] true if the node's pointer next[i] is set to be removed/change.
 * LeapNode[] next that keeps the next node in each level,
 * trie for finding the values in the node.
 * we use volatile variables are been use for cache coherence and synchronization between multiple threads.
 */
public class LeapNode {
	volatile boolean live ;
	volatile boolean[] Marks = new boolean[LeapList.MAX_LEVEL];
	volatile public long low;
	volatile public long high;
	volatile public int count;
	volatile byte level;
	public LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	private LeapNode[] next =  new LeapNode [LeapList.MAX_LEVEL];
	Trie trie;
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs) {
		this();
		this.live = live;
		this.level = level;
		this.low = low;
		this.high = high;
		this.count = count;
		
		if (sortedPairs != null)
		{
			this.data= sortedPairs;
			trie = new Trie(sortedPairs, sortedPairs.length);
		}
	}
	
	public LeapNode (){
		for (int i = 0 ; i <LeapList.MAX_LEVEL ; i ++ )
		{
			Marks[i]= false;
		}
		this.live = false;
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++)
		{
			data[i] = new LeapSet(0,0);
		}
	}
	
	//get the next node from the given level.
	public LeapNode getNext (int i){
		return next[i];
	}
	//set the next node in the given level to the give node
	public void setNext(int level, LeapNode node){
		next[level] = (node);
	}
	
}
