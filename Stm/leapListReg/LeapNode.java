package leapListReg;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import utils.LeapSet;
import utils.Trie;

public class LeapNode {
	volatile boolean live = true;
	volatile public long low;
	volatile public long high;
	volatile public int count;
	volatile byte level;
	volatile public boolean Marked;
	public LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	public LeapNode [] next = new LeapNode [LeapList.MAX_LEVEL];
	Trie trie;
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs) {
		this();
		this.live=live;
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
			
		this.Marked = false;
		this.live = false; 
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	/*public LeapNode getNext (int i){
		return next.get(i).get();
	}
	
	public void setNext(int level, LeapNode node){
		next.get(level).set(node);
	}*/
	
}
