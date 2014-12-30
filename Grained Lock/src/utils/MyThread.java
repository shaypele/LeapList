package utils;



import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;


public class MyThread extends Thread {
	
	LeapListDB db ;
	Queue <Integer> keyQ;
	Queue <Integer> opQ; 
	int keyRange;
	
	public MyThread(LeapListDB db, Queue<Integer> keyQ, Queue<Integer> opQ, int keyRange){
		this.db = db;
		this.keyQ = keyQ;
		this.opQ = opQ;
		this.keyRange = keyRange;
	}
	
	void insert(int key){
		LeapList list0 = db.GetListByIndex(0);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{key, }, new Object[]{String.valueOf(key)},1);
	}
	
	void remove(int key){
		LeapList list0 = db.GetListByIndex(0);
		db.leapListRemove(new LeapList[] {list0}, new long[]{key,  }, 1);
	}
	
	Object lookup (int key){
		LeapList list0 = db.GetListByIndex(0);
		return db.lookUp(list0, key);
	}
	
	Object [] rq (int lowKey, int highKey){
		LeapList list0 = db.GetListByIndex(0);
		return db.RangeQuery(list0, lowKey, highKey);
	}
	
	public void run(){
		while (!opQ.isEmpty()){	
			
			switch (opQ.remove()){
				case 0:
					lookup(keyQ.remove());
					break;
				case 1:
					int tmp = keyQ.remove();
					rq(tmp, tmp + keyRange);
					break;
				case 2:
					insert(keyQ.remove());
					break;
				case 3:
					remove(keyQ.remove());
					break;
				}
		}
		
		return;
	}
}
