package utils;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
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
	int numberOfLists;
	long [] keys;
	LeapList [] lists;
	Object [] vals;
	
	PaddedPrimitiveNonVolatile<Boolean> done;
	int indexStart;
	int indexStop;
	
	
	public MyThread(LeapListDB db, int [] keyArr, int [] opArr, int keyRange, PaddedPrimitiveNonVolatile<Boolean> done,
			int indexStart, int indexStop, int numberOfLists){
		this.db = db;
		this.keyArr = keyArr;
		this.opArr = opArr;
		this.keyRange = keyRange;
		this.done = done;
		this.indexStart = indexStart;
		this.indexStop = indexStop;
		this.numberOfLists = numberOfLists;
		
		keys = new long [numberOfLists];
		lists = new LeapList[numberOfLists];
		vals = new Object [numberOfLists];
		
		for (int i = 0; i < numberOfLists; i++) {
			lists[i] = db.GetListByIndex(i);
		}
		
		BuildArrOptions();
		
	}
	
	
	void insert(){
		int listNum = ThreadLocalRandom.current().nextInt(0,2);
		for (int i = 0; i < numberOfLists; i++) {
			vals[i] = String.valueOf(keys[i]);
		}
		db.leapListUpdate(arrOptions.get(listNum), keys, vals, numberOfLists);
		updateCounter ++;
	}
	
	void remove(){
		int listNum = ThreadLocalRandom.current().nextInt(0,2);
		db.leapListRemove(arrOptions.get(listNum), keys, numberOfLists);
		removeCounter ++;
	}
	
	Object lookup (int key){
		LeapList list0 = db.GetListByIndex(key%numberOfLists);
		Object tmp = db.lookUp(list0, key);
		lookCounter ++;
		return tmp;
	}
	
	Object [] rq (int key){
		LeapList list0 = db.GetListByIndex(key%numberOfLists);
		Object [] tmp = db.RangeQuery(list0, key, key + keyRange);
		rangeCounter ++;
		return tmp;
	}
	
	public void run(){
		int i = indexStart;
		int j = indexStart;
		while (true) {
			if (done.value){
				break;
			}
			if (i >= indexStop){
				System.out.print("array too small!!! increase array size");
			}
			
			switch (opArr[j]){
				case 0:
					lookup(keyArr[i]);
					i++;
					break;
				case 1:
					rq(keyArr[i]);
					i++;
					break;
				case 2:
					for (int k = 0; k < numberOfLists; k++) {
						keys[k] = keyArr[i + k];
					}
					insert();
					i+=numberOfLists;
					break;
				case 3:
					for (int k = 0; k < numberOfLists; k++) {
						keys[k] = keyArr[i + k];
					}
					remove();
					i+=numberOfLists;
					break;
				}
			j++;
		}
		
		totCounter = rangeCounter + lookCounter + updateCounter +removeCounter;
		
		return;
	}
	
	HashSet<String> arrOptionsHelper ;
	
	ArrayList<LeapList[]> arrOptions;
	
	// Build 2 options for lists array 
	 public  void BuildArrOptions()
		{
			arrOptions = new ArrayList<LeapList[]>();
			arrOptions.add(new LeapList[] {lists[0],lists[1],lists[2],lists[3]});
			arrOptions.add(new LeapList[] {lists[3],lists[2],lists[1],lists[0]});

			
		}
	
	
}
