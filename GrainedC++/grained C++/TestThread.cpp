#include "TestThread.h"



	
	
	 TestThread::TestThread(LeapListDB* db, int funcToRun, long* arrKeys, int arrStart,int arrEnd , int size){
		this->db = db;
		this->funcToRun = funcToRun;
		this->arrKeys = arrKeys;
		this->arrStart = arrStart;
		this->arrEnd = arrEnd;
		this->size = size;
	}
	
	void TestThread::insert1(){
		/*volatile LeapList* list0 = db->GetListByIndex(0);
		LeapList ** arrLists = new LeapList*[1]; 
		//db->leapListUpdate(new LeapList* {list0}, new long*{3, }, new void**{"1st"},1);
		db->leapListUpdate(arrLists, new long*{5, }, new void**{"3rd"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{90, }, new void**{"5th"},1);
		db->leapListUpdate(arrLists, new long*{7}, new void**{"4th"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{4, }, new void**{"2nd"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{7, }, new void**{"I'm am 7"},1);*/
	}
	
	void TestThread::insert2(){
		/*LeapList* list0 = db->GetListByIndex(0);
		db->leapListUpdate(new LeapList** {list0}, new long*{3, }, new void**{"1st"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{5, }, new void**{"3rd"},1);
		db->leapListUpdate(new LeapList** {list0}, new long*{90, }, new void**{"5th"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{7}, new void**{"4th"},1);
		db->leapListUpdate(new LeapList** {list0}, new long*{4, }, new void**{"2nd"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{7, }, new void**{"I'm am 7"},1);*/
	}
	
	void TestThread::insert3(){
		/*LeapList* list0 = db->GetListByIndex(0);
		//db->leapListUpdate(new LeapList* {list0}, new long*{3, }, new void**{"1st"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{5, }, new void**{"3rd"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{90, }, new void**{"5th"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{7}, new void**{"4th"},1);
		//db->leapListUpdate(new LeapList* {list0}, new long*{4, }, new void**{"2nd"},1);
		db->leapListUpdate(new LeapList** {list0}, new long*{7, }, new void**{"I'm am 7"},1);*/
	}
	
	 void TestThread::run(){
			
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
		
		
		System->out->println(" Rangen ");
		void** arr =  list0->RangeQuery(7, 90);
		for (void* obj : arr){
			System->out.println(" Item Is " + obj.toString() + "\n");
		}
		
		System.out.println(" Remove ");
		//db.leapListRemove(new LeapList* {list0}, new long*{90, },1);
		db.leapListRemove(new LeapList* {list0}, new long*{5, },1);
		*//*db.leapListRemove(new LeapList* {list0}, new long*{7},1);
		db.leapListRemove(new LeapList* {list0}, new long*{4, },1);
		db.leapListRemove(new LeapList* {list0}, new long*{3, },1);
		db.leapListRemove(new LeapList* {list0}, new long*{5, },1);
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
		while (head!= 0);
		
		System.out.println("LOOKUP: \n");
		void* obji = list0.lookUp(7);
		String strLookup = " I'm 0";
		if ( obji != 0 )
		{
			strLookup = obji.toString();
		}
		System.out.println( "Result IS " +  strLookup);
		
		
		System.out.println("XXXXXX");*/
	}
	
	 void TestThread::getRQ() {
		LeapList* list0 = db->GetListByIndex(0);
		int cell1,cell2,low,high;
		CCP::Random rand ;
		cell1 = arrKeys[rand.nextInt(0,size )];
		cell2 = arrKeys[rand.nextInt(0,size )];
		if (cell1 > cell2 ){
			high = cell1;
			low = cell2;
		}else{
			high = cell2;
			low = cell1;
		}
		for (int i = 0 ; i < 100 ; i++){
			
			db->RangeQuery(list0, low, high);
		}
		
	}

	 void TestThread::removeRand() {
		 int numChanges = 1;
		LeapList* list0 = db->GetListByIndex(0);
		LeapList** lists = new LeapList*[numChanges];
		long* keys = new long[numChanges];
		lists[0] = list0;
		for (int i = arrStart ; i < arrEnd ; i++){
			keys[0] = arrKeys[i];
			db->leapListRemove(lists, keys,numChanges);
		}
		
	}

	 void TestThread::insertRand() {
		  int numChanges = 1;
		LeapList* list0 = db->GetListByIndex(0);
		LeapList** lists = new LeapList*[numChanges];
		long* keys = new long[numChanges];
		lists[0] = list0;
		for (int i = arrStart ; i < arrEnd ; i++){
			keys[0] = arrKeys[i];
			db->leapListUpdate(lists, keys,(void**) keys,1);
		}
		
	}


TestThread::~TestThread(void)
{
}
