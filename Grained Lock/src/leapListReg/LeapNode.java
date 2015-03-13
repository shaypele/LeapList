package leapListReg;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
 * volatile boolean Marked that represent if the node is set to be removed for lazy implementation.
 * ArrayList<AtomicReference<LeapNode>> next that keeps the next node in each level,
 * trie for finding the values in the node.
 * we use volatile variables and atomicMarkedReference are been use for cache coherence and synchronization between multiple threads.
 */


public class LeapNode {
	volatile boolean live = true;
	volatile long low;
	volatile public long high;
	volatile public int count;
	volatile byte level;
	public LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	private ArrayList<AtomicReference<LeapNode>> next ;
	Trie trie;
	volatile public boolean Marked;
	
	public final ReentrantLock nodeLock = new ReentrantLock();
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs) {
		this();
		this.live = live;
		this.level = level;
		this.low = low;
		this.high = high;
		this.count = count;
		this.level = level;
		
		
		if (sortedPairs != null)
			trie = new Trie(sortedPairs, sortedPairs.length);
	}
	
	public LeapNode (){
		next=new  ArrayList<AtomicReference<LeapNode>>(LeapList.MAX_LEVEL);
		for (int i = 0 ; i <LeapList.MAX_LEVEL ; i ++ ){
			next.add(i, new AtomicReference<LeapNode>());
		}
			
		this.Marked = false;
		this.live = false; 
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	//try to lock the node if succeed then hold the lock and return true else return false.
	public boolean tryLock(){
		return nodeLock.tryLock();
	}
	
	//hold the node lock
	public void lock(){
		nodeLock.lock();
	}
	
	//free the node lock
	public void unlock(){
			nodeLock.unlock();
	}
	
	//get the next node from the given level.
	public LeapNode getNext (int i){
		return next.get(i).get();
	}
	
	//set the next node in the given level to the give node
	public void setNext(int level, LeapNode node){
		next.get(level).set(node);
	}
	
	// go over all elements in the data array of n in order to find the given key.
	public short findIndex(long key){
		short retInd = -1;
		for (short i=0 ; i < LeapList.NODE_SIZE ; i++){
			if (data[i].key == key){
				retInd = i;
				break;
			}
		}
		return retInd;
	}
	
}
