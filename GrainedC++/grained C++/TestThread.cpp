#include "TestThread.h"
#include <random>
TestThread::TestThread(void)
{
}


	LeapListDB* db ;
	int funcToRun;
	long[] arrKeys;
	int arrStart;
	int arrEnd;
	
	 TestThread(LeapListDB* db, int funcToRun, long[] arrKeys, int arrStart,int arrEnd){
		this->db = db;
		this->funcToRun = funcToRun;
		this->arrKeys = arrKeys;
		this->arrStart = arrStart;
		this->arrEnd = arrEnd;
	}
	
	void insert1(){
		LeapList* list0 = db.GetListByIndex(0);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
	}
	
	void insert2(){
		LeapList* list0 = db.GetListByIndex(0);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
	}
	
	void insert3(){
		LeapList* list0 = db.GetListByIndex(0);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		//db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
	}
	
	 void run(){
			
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
	
	 void getRQ() {
		LeapList* list0 = db.GetListByIndex(0);
		int cell1,cell2;
		Random rand = new Random();
		cell1 = rand.nextInt(arrKeys->length );
		for (int i = 0 ; i < 100 ; i++){
			cell2 = rand.nextInt(arrKeys->length - cell1) + cell1;
			db.RangeQuery(list0, cell1, cell2);
		}
		
	}

	 void removeRand() {
		LeapList* list0 = db.GetListByIndex(0);
		for (int i = arrStart ; i < arrEnd ; i++){
			
			db.leapListRemove(new LeapList[] {list0}, new long[]{arrKeys[i], },1);
		}
		
	}

	 void insertRand() {
		LeapList* list0 = db.GetListByIndex(0);
		for (int i = arrStart ; i < arrEnd ; i++){
			
			db.leapListUpdate(new LeapList[] {list0}, new long[]{arrKeys[i], }, new Object[]{arrKeys[i]},1);
		}
		
	}


TestThread::~TestThread(void)
{
}
