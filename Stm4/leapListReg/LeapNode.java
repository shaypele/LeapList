package leapListReg;
import utils.LeapSet;
import utils.Trie;







public class LeapNode {
	volatile boolean live ;
	volatile boolean[] lives = new boolean[LeapList.MAX_LEVEL];
	volatile boolean[] Marks = new boolean[LeapList.MAX_LEVEL];
	volatile public long low;
	volatile public long high;
	volatile public int count;
	volatile byte level;
	volatile public boolean Marked ;
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
		for (int i = 0 ; i <LeapList.MAX_LEVEL ; i ++ ){
			//next[i] = new LeapNode();
			lives[i] = false;
			Marks[i]= false;
		}
			
		this.Marked = false;
		this.live = false;
		for (int i = 0 ; i < LeapList.NODE_SIZE ; i ++){
			data[i] = new LeapSet(0,0);
		}
	}
	
	public LeapNode getNext (int i){
		return next[i];
	}
	
	public void setNext(int level, LeapNode node){
		next[level] = (node);
	}
	
}
