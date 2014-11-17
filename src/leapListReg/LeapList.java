package leapListReg;

import java.util.Random;

import utils.*;

public class LeapList {
	final static int MAX_LEVEL = 10;
	final static int MAX_ROW = 4;
	final static int NODE_SIZE = 300;
	
	LeapNode head;

	public LeapList () {
		head = new LeapNode (true, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, MAX_LEVEL, null);
		LeapNode tail = new LeapNode (true, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 0, MAX_LEVEL, null);
		for (int i = 0; i < MAX_LEVEL; i++){
			head.next[i] = tail;
		}
	}
	
 	static int getLevel(){
		Random rand = new Random();
		long r = rand.nextLong();
		int l = 1;
		r = (r >> 4) & ((1 << (MAX_LEVEL - 1)) -1);
		while ((r & 1)  > 0){
			l++;
			r >>= 1;
		}
		return l;
	}
	
	static LeapNode searchPredecessor (LeapList l, int key, LeapNode[] pa, LeapNode[] na){
		
		LeapNode x, x_next;
		
		x = l.head;
		
		for (int i = MAX_LEVEL -1; i >= 0; i--) {
			while (true){
				x_next = x.next[i];
				if (x_next.high > key)
					break;
				else
					x = x_next;
			}
			pa[i] = x;
			na[i] = x_next;
		}
		
		return na[0];
	}
	
	static Object lookUp (LeapList l, int key){
		LeapNode [] na = new LeapNode[MAX_LEVEL];
		LeapNode [] pa = new LeapNode[MAX_LEVEL];
		LeapNode ret = searchPredecessor(l, key, pa, na);
		return ret.data[ret.trie.trieFindVal(key)].value;
	}

	static void leapListUpdate (LeapList [] ll, int [] keys, Object [] values, int size){
		LeapNode[][] pa = new LeapNode[MAX_ROW][MAX_LEVEL];
		LeapNode[][] na = new LeapNode[MAX_ROW][MAX_LEVEL];
		LeapNode[] n = new LeapNode[MAX_ROW];
		LeapNode [][] newNode = new LeapNode[MAX_ROW][2];
		int [] maxHeight = new int[MAX_ROW];
		boolean [] split = new boolean [MAX_ROW];
		boolean [] changed = new boolean [MAX_ROW];
		
		for(int i = 0; i < MAX_ROW; i++){
			newNode[i][0] = new LeapNode();
			newNode[i][2] = new LeapNode();
		}
		
		updateSetup (ll, keys, values, size, pa, na, n, newNode, maxHeight, split, changed);
		updateLT (size, pa, na, n, newNode, maxHeight, changed);
		updateRelease (size, pa, na, n, newNode, split, changed);
		
	}
	
	static void updateSetup (LeapList[] ll, int [] keys, Object [] values, int size, LeapNode[][] pa, LeapNode[][] na, 
										LeapNode[] n, LeapNode [][] newNode, int [] maxHeight, boolean[] split, boolean[] changed){
		
		for (int i = 0; i < size; i ++){
			searchPredecessor(ll[i], keys [i], pa[i], na[i]);	
			n[i] = na[i][0];
			if (n[i].count == NODE_SIZE){
				split[i] = true;
				newNode[i][1].level = n[i].level;
				newNode[i][0].level = getLevel();
				maxHeight[i] = (newNode[i][0].level > newNode[i][1].level) ? newNode[i][0].level: newNode[i][1].level;
			}
			else{
				split[i] = false;
				newNode[i][0].level = n[i].level;
				maxHeight[i] = newNode[i][0].level;
			}
			changed [i] = insert(newNode[i], n[i], keys[i], values[i], split[i]);
		}
	}
	
	static boolean insert (LeapNode[] newNode, LeapNode n, int key, Object val, boolean split){
		boolean changed = false;
		int m = 0;
		int i = 0;
		int j = 0;
	
		
		if (split){
			newNode[0].low = n.low;
			newNode[0].count = n.count;
			newNode[1].high = n.high;
			newNode[1].count = n.count - (NODE_SIZE/2);
		}
		else
		{
			newNode[0].low = n.low;
			newNode[0].high = n.high;
			newNode[0].count = n.count;
		}
		if (n.count == 0){
			newNode[m].data[0].key = key;
			newNode[m].data[0].value = val;
			newNode[m].count = 1;
			changed = true;
			newNode[m].trie = new Trie(key, (short)0);
		}
		else{
			for (j = 0; j < n.count; i++, j++){
				if (n.data[j].key == key){     //there is an int override int the prof. cod. not sure what it does.
					newNode[m].data[i].value = val;
					changed = true;
				}
				
				if ((!changed) && (n.data[j].key > key)){
					newNode[m].data[i].key = key;
					newNode[m].data[i].value = val;
					newNode[m].count++;
					changed = true;
					
					if ((m != 1) && split && (newNode[0].count == (i+1))){
						newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
						i = -1;
						m = m + 1;
					}
					
					i++;
				}
				
				newNode[m].data[i].key = n.data[j].key;
				newNode[m].data[i].value = n.data[j].value;
				
				if ((m != 1) && split && (newNode[0].count == (i+1))){
					newNode[m].high = newNode[m+1].low = newNode[m].data[i].key;
					i = -1;
					m = m+1;
				}
			}
			
			if (!changed){
				newNode[m].count++;
				newNode[m].data[i].key = key;
				newNode[m].data[i].value = val;
				changed = true;
			}
			
			if (split){
				newNode[0].trie = new Trie(newNode[0].data, newNode[0].count);
				newNode[1].trie = new Trie(newNode[1].data, newNode[1].count);
			}
			else{
				newNode[m].trie = new Trie(newNode[m].data, newNode[m].count);
			}
		}
		return changed;
	}
	
	//runs test for transactional memory multi thread
	static void updateLT (int size, LeapNode [][] pa, LeapNode [][] na, LeapNode[] n, LeapNode[][] newNode, int[] maxHeight,
								boolean[] changed){
	}
	
	static void updateRelease (int size, LeapNode[][] pa, LeapNode[][] na, LeapNode[] n, LeapNode[][] newNode, boolean[] split,
									boolean[] changed){
		for (int j = 0; j < MAX_ROW; j++){
			int i = 0;
			
			if (changed[j]){
				if (split[j]){
					if (newNode[j][1].level > newNode[j][0].level){
						for (i = 0; i < newNode[j][0].level; i++){
							newNode[j][0].next[i] = newNode[j][1];
                            newNode[j][1].next[i] = n[j].next[i];
						}
						 for (; i < newNode[j][1].level; i++)
	                            newNode[j][1].next[i] = n[j].next[i];
					}
					else
                    {   
                        for (i = 0; i < newNode[j][1].level; i++)
                        {
                            newNode[j][0].next[i] = newNode[j][1];
                            newNode[j][1].next[i] = n[j].next[i];
                        }
                        for (; i < newNode[j][0].level; i++)
                            newNode[j][0].next[i] = na[j][i];
                    }
				}
				else
                {
                    for (i = 0; i < newNode[j][0].level; i++)
                    {
                        newNode[j][0].next[i] = n[j].next[i];
                    }
                }
				
				for(i=0; i < newNode[j][0].level; i++)
                {
                    pa[j][i].next[i] = newNode[j][0];
                }
                if (split[j] && (newNode[j][1].level > newNode[j][0].level)){
                    for(; i < newNode[j][1].level; i++)
                    { 	
                        pa[j][i].next[i] = newNode[j][1];
                    }
                }

			}
		}
	}
	

}
