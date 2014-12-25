package utils;



import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import sun.swing.text.CountingPrintable;

import leapListReg.LeapList;
import leapListReg.LeapListDB;


public class TestThread extends Thread {
	
	LeapListDB db ;
	int funcToRun;
	long[] arrKeys;
	int arrStart;
	int arrEnd; 
	
	public TestThread(LeapListDB db, int funcToRun, long[] arrKeys, int arrStart,int arrEnd){
		this.db = db;
		this.funcToRun = funcToRun;
		this.arrKeys = arrKeys;
		this.arrStart = arrStart;
		this.arrEnd = arrEnd;
	}
	
	void insert1(){
		LeapList list0 = db.GetListByIndex(0);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
	}
	
	void insert2(){
		LeapList list0 = db.GetListByIndex(0);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
	}
	
	void insert3(){
		LeapList list0 = db.GetListByIndex(0);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
	}
	
	public void run(){
			
		switch ( funcToRun ){
		case 1:
			insert1();
			break;
		case 2:
			insert2();
			break;
		case 3:
			insert3();
			break;
		case 4:
			insertRand();
			break;
		case 5:
			removeRand();
			break;
		case 6:
			getRQ();
			break;
		case 7:
			LookUp();
			break;
		case 8 :
			insertRand2();
			break;
		case 9 :
			insertRand3();
			break;
		}
		
		
		return;
		/*
		
		
		System.out.println(" Rangen ");
		Object[] arr =  list0.RangeQuery(7, 90);
		for (Object obj : arr){
			System.out.println(" Item Is " + obj.toString() + "\n");
		}
		
		System.out.println(" Remove ");
		//db.leapListRemove(new LeapList[] {list0}, new long[]{90, },1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{5, },1);
		*//*db.leapListRemove(new LeapList[] {list0}, new long[]{7},1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{4, },1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{3, },1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{5, },1);
		*//*
		head = list0.GetHeadNode();
		do 
		{
			System.out.println("new node \n");
			for (int i = 0 ; i < head.count ; i ++){
				System.out.println(" Item Is " + head.data[i].value + "\n"); 
			}
			head = head.next[0];
		}
		while (head!= null);
		
		System.out.println("LOOKUP: \n");
		Object obji = list0.lookUp(7);
		String strLookup = " I'm null";
		if ( obji != null )
		{
			strLookup = obji.toString();
		}
		System.out.println( "Result IS " +  strLookup);
		
		
		System.out.println("XXXXXX");*/
	}
	
	private void LookUp() {
		
		Random rand = new Random();
		LeapList list0 = db.GetListByIndex(0);
		long look1 = rand.nextInt(Integer.MAX_VALUE);
		long look2 = arrKeys[20];
		System.out.println("*****************************    " + db.lookUp(list0, look1));
		System.out.println("*****************************    " + db.lookUp(list0, look2));
	}

	private void getRQ() {
		
		Random rand = new Random();
		LeapList list0 = db.GetListByIndex(0);
		LeapList list1 = db.GetListByIndex(1);
		int cell1,cell2,high,low;
		cell1 = rand.nextInt(Integer.MAX_VALUE);
		cell2 = rand.nextInt(Integer.MAX_VALUE);
			
		if(cell1>cell2){
			high = cell1;
			low=cell2;
		}
		else {
			high = cell2;
			low=cell1;
		}
		
			System.out.println(" Range in list 0 between: " + low + "  and  " + high);
			Object[] arr =	db.RangeQuery(list0, low, high);
			
			for (Object obj : arr){
				System.out.println(" Item Is " + obj.toString() + "\n");
			}
			System.out.println(" Range in list 1 between: " + low + "  and  " + high);
			 arr =	db.RangeQuery(list1, low, high);
			
			for (Object obj : arr){
				System.out.println(" Item Is " + obj.toString() + "\n");
			}
		
	}

	private void removeRand() {
		
		
		for (int i = arrStart ; i < arrEnd ; i++){
			
			db.leapListRemove(db.LeapLists, new long[]{arrKeys[i],arrKeys[i] ,arrKeys[i],arrKeys[i]},4);
		}
		
	}

	
	private void insertRand2() {

	
		
		
LeapList[] lists = new LeapList[10];
		
		for (int i = 0; i < lists.length; i++) {
			lists[i] = db.GetListByIndex(i);
		}
		
		LeapList[] ListToSend = new LeapList []{lists[4],lists[5],lists[7],lists[2],lists[0],lists[3],lists[8]};
		long[] keys = new long[7];//{arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i]};
		Object[] vals = new Object[7];//{arrKeys[i],arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i]};
		
		int sizeToRun = 4;
		
		for (int i = arrStart ; i < arrEnd ; i++){
			for (int j = 0 ; j < sizeToRun ; j++){
				keys[j] = arrKeys[i];
				vals[j] = arrKeys[i];
			}
			db.leapListUpdate(ListToSend,keys , vals,sizeToRun);
		}
	}
		
		private void insertRand3() {

	
			
			LeapList[] lists = new LeapList[10];
			
			for (int i = 0; i < lists.length; i++) {
				lists[i] = db.GetListByIndex(i);
			}
			
			LeapList[] ListToSend = new LeapList []{lists[2],lists[3],lists[7],lists[8],lists[9],lists[5],lists[0]};
			long[] keys = new long[7];//{arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i]};
			Object[] vals = new Object[7];//{arrKeys[i],arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i]};
			
			int sizeToRun = 4;
			
			for (int i = arrStart ; i < arrEnd ; i++){
				for (int j = 0 ; j < sizeToRun ; j++){
					keys[j] = arrKeys[i];
					vals[j] = arrKeys[i];
				}
				db.leapListUpdate(ListToSend,keys , vals,sizeToRun);
			}
		
	}
	
	
		private ThreadLocalRandom rand = ThreadLocalRandom.current() ;
		
	private void insertRand() {

		LeapList[] lists = new LeapList[10];
		
		for (int i = 0; i < lists.length; i++) {
			lists[i] = db.GetListByIndex(i);
		}
		
		LeapList[] ListToSend = new LeapList []{lists[0],lists[1],lists[2],lists[3],lists[4],lists[5],lists[6]};
		long[] keys = new long[7];//{arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i]};
		Object[] vals = new Object[7];//{arrKeys[i],arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i], arrKeys[i]};
		
		int sizeToRun = 4;
		int countUP = 0, countRem = 0;
		for (int i = arrStart ; i < arrEnd ; i++){
			for (int j = 0 ; j < sizeToRun ; j++){
				keys[j] = arrKeys[i];
				vals[j] = arrKeys[i];
			}
			//if (arrKeys[i] % 2 == 0){
			if (rand.nextBoolean() ){
				countUP++;
				db.leapListUpdate(ListToSend,keys , vals,sizeToRun);
			}
			else{
				countRem++;
				db.leapListRemove(ListToSend,keys , sizeToRun);
			}
		}
		
		System.out.println("UP : " + countUP + " REM : " + countRem );
		
	}
}

