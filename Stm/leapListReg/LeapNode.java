package leapListReg;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import utils.LeapSet;
import utils.Trie;

public class LeapNode {
	volatile AtomicBoolean live = new AtomicBoolean();
	volatile AtomicBoolean[] lives = new AtomicBoolean[LeapList.MAX_LEVEL];
	volatile AtomicBoolean[] Marks = new AtomicBoolean[LeapList.MAX_LEVEL];
	volatile public long low;
	volatile public long high;
	volatile public int count;
	volatile byte level;
	volatile public AtomicBoolean Marked = new AtomicBoolean();
	public LeapSet [] data = new LeapSet[LeapList.NODE_SIZE]; //the array must be sorted by keys so that LeapSet with the smallest key is at LeapSet[0] etc.
	private ArrayList<AtomicReference<LeapNode>> next ;
	Trie trie;
	
	public LeapNode (boolean live, long low, long high, int count, byte level, LeapSet[] sortedPairs) {
		this();
		this.live.set(live);
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
		next=new  ArrayList<AtomicReference<LeapNode>>(LeapList.MAX_LEVEL);
		for (int i = 0 ; i <LeapList.MAX_LEVEL ; i ++ ){
			next.add(i, new AtomicReference<LeapNode>());
			lives[i] = new AtomicBoolean(false);
			Marks[i]= new AtomicBoolean(false);
		}
			
		this.Marked.set(false);
		this.live.set(false);
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	public LeapNode getNext (int i){
		return next.get(i).get();
	}
	
	public void setNext(int level, LeapNode node){
		next.get(level).set(node);
	}
	
}
