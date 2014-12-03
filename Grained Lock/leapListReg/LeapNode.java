package leapListReg;

import java.util.concurrent.locks.ReentrantLock;

import utils.LeapSet;
import utils.Trie;

public class LeapNode {
	boolean live = true;
	long low;
	public long high;
	public int count;
	byte level;
	public LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	public LeapNode [] next = new LeapNode [LeapList.MAX_LEVEL];
	Trie trie;
	public boolean Marked;
	
	public final ReentrantLock nodeLock = new ReentrantLock();
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs) {
		this();
		this.level = level;
		this.low = low;
		this.high = high;
		this.count = count;
		this.level = level;
		
		if (sortedPairs != null)
			trie = new Trie(sortedPairs, sortedPairs.length);
	}
	
	public LeapNode (){
		this.Marked = false;
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	public boolean tryLock(){
		return nodeLock.tryLock();
	}
	
	public void lock(){
		nodeLock.lock();
	}
	
	public void unlock(){
		
		//if (nodeLock.isHeldByCurrentThread())
			nodeLock.unlock();
	}
	
}
