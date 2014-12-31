package utils;



import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;


public class MyThread extends Thread {
	
	LeapListDB db ;
	int [] keyArr;
	int [] opArr; 
	int keyRange;
	int totCounter = 0;
	int lookCounter = 0;
	int updateCounter = 0;
	int rangeCounter = 0;
	int removeCounter = 0;
	
	PaddedPrimitiveNonVolatile<Boolean> done;
	int indexStart;
	int indexStop;
	
	public MyThread(LeapListDB db, int [] keyArr, int [] opArr, int keyRange, PaddedPrimitiveNonVolatile<Boolean> done, int indexStart, int indexStop){
		this.db = db;
		this.keyArr = keyArr;
		this.opArr = opArr;
		this.keyRange = keyRange;
		this.done = done;
		this.indexStart = indexStart;
		this.indexStop = indexStop;
	}
	
	void insert(int key){
		LeapList list0 = db.GetListByIndex(0);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{key, }, new Object[]{String.valueOf(key)},1);
		updateCounter++;
	}
	
	void remove(int key){
		LeapList list0 = db.GetListByIndex(0);
		db.leapListRemove(new LeapList[] {list0}, new long[]{key,  }, 1);
		removeCounter++;
	}
	
	Object lookup (int key){
		LeapList list0 = db.GetListByIndex(0);
		lookCounter++;
		return db.lookUp(list0, key);
	}
	
	Object [] rq (int lowKey, int highKey){
		LeapList list0 = db.GetListByIndex(0);
		rangeCounter++;
		return db.RangeQuery(list0, lowKey, highKey);
	}
	
	public void run(){
		int i = indexStart;
		while (true) {
			if (done.value){
				break;
			}
			if (i == indexStop){
				System.out.print("array too small!!! increase array size");
			}
			
			switch (opArr[i]){
				case 0:
					lookup(keyArr[i]);
					break;
				case 1:
					int tmp = keyArr[i];
					rq(tmp, tmp + keyRange);
					break;
				case 2:
					insert(keyArr[i]);
					break;
				case 3:
					remove(keyArr[i]);
					break;
				}
			i++;
		}
		
		totCounter = rangeCounter + lookCounter + updateCounter +removeCounter;
		
		return;
	}
}
