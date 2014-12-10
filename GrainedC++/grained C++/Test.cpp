#include "Test.h"


Test::Test(void)
{
}

static void doTest(){
		LeapListDB* db =	new LeapListDB();
		LeapList* list0 = db->GetListByIndex(0);
		int arrSize = 600;
		long* arrRand = new long[arrSize];
		CCP::Random* rand = new CCP::Random();
		for (int i = 0 ; i < arrSize ; i++){
			arrRand [i] = std::abs(rand->nextLong(0,LONG_MAX)); 
		}
		TestThread* thread1 = new TestThread( db , 4 , arrRand, 0 , arrSize / 3,arrSize / 3  );
		TestThread* thread2 = new TestThread( db , 4 , arrRand, arrSize / 3 ,arrSize * 2 /3,arrSize / 3  );
		TestThread* thread3 = new TestThread( db , 4 , arrRand, arrSize * 2 /3, arrSize ,arrSize / 3  );
		TestThread* threadRem1 = new TestThread( db , 5 , arrRand, arrSize / 3 ,arrSize * 2 /3,arrSize / 3  );
		TestThread* threadRem2 = new TestThread( db , 5 , arrRand, arrSize * 2 /3, arrSize ,arrSize / 3  );
		TestThread* threadRQ = new TestThread( db , 6 , arrRand, 0 , arrSize / 3,arrSize / 3  );
		const clock_t start = clock();
		
		thread1->start();
		thread2->start();
		thread3->start();

		
		
			thread1->join();
			thread2->join();
			thread3->join();
	      
	      
	      
	      //System->out->println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	      LeapNode* head = list0->GetHeadNode();
	      long min = -1 ;
	      int j = 0;
	      int totItems = 0;
			do 
			{
				//System->out->println("new node " + j + "\n");
				int k = 0;
				for (int i = 0 ; i < head->count ; i ++){
					//System->out->println(" Item Is " + head->data[i]->value + "\n");
					k++;
					long val = (long) head->data[i]->value; 
					if ( ( val > min))
					{
						min = val ;
					}
					else
					{
						printf(" ERROR not sorted! ");
						return;
					}
					
					if ( val > head->high ){
						printf(" ERROR not in range! ");
						return;
					}
				}
				//printf("num of items " + k + "\n");
				totItems+=k;
				j++;
				head = head->getNext(0);
			}
			while (head!= 0);
			
			printf(" Total number of items before delete is %d", totItems);
	     
	    threadRem1->start();
	    threadRem2->start();
	    threadRQ->start();
	    	threadRem1->join();
	    	threadRem2->join();
	    	threadRQ->join();
	    
	      
		  const clock_t end = clock();
	      
	      printf(" Fine Grained Lock :  Time Elapsed : %lf\n" , (float(end - start) / CLOCKS_PER_SEC / 1000) / 1000 );
	       head = list0->GetHeadNode();
	       min = 0 ;
	       j = 0;
	       totItems = 0;
			do 
			{
				printf("new node %d\n",j);
				int k = 0;
				for (int i = 0 ; i < head->count ; i ++){
					//System->out->println(" Item Is " + head->data[i]->value + "\n");
					k++;
					long val = (long) head->data[i]->value; 
					if ( ( val > min))
					{
						min = val ;
					}
					else
					{
						printf(" ERROR not sorted! ");
						return;
					}
					
					if ( val > head->high ){
						printf(" ERROR not in range! ");
						return;
					}
				}
				printf("num of items %d \n" , k );
				totItems+=k;
				j++;
				head = head->getNext(0);
			}
			while (head!= 0);
			
			printf(" Total number of items is %d" , totItems);
			
			printf(" OK data is sorted! ");
	}
	
	 static void main(void) {
		
		for (int i=0 ; i < 10 ; i ++){
			doTest();
		}
	}

Test::~Test(void)
{
}
