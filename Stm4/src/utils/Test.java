package utils;



import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;



public class Test {

	public static void doTest(){
		LeapListDB db =	new LeapListDB();
		int arrSize = 300;
		long[] arrRand = new long[arrSize];
		Random rand = new Random();
		for (int i = 0 ; i < arrSize ; i++){
			arrRand [i] = Math.abs(rand.nextInt()); 
		}
		TestThread thread1 = new TestThread( db , 4 , arrRand, 0 , arrSize /3);
		TestThread thread2 = new TestThread( db , 8 , arrRand, arrSize /3 ,arrSize * 2/3);
		TestThread thread3 = new TestThread( db , 9 , arrRand, arrSize * 2/3 ,arrSize);
		TestThread thread4 = new TestThread( db , 8 , arrRand, arrSize  /2, arrSize );
		TestThread threadRem2 = new TestThread( db , 5 , arrRand, arrSize / 3 ,arrSize * 2 /3);
		TestThread threadRem3 = new TestThread( db , 5 , arrRand, arrSize * 2 /3, arrSize );
		TestThread threadRQ = new TestThread( db , 6 , arrRand, 0 , arrSize / 3);
		TestThread threadLook1 = new TestThread( db , 7 , arrRand, 0 , arrSize / 3);
		long start = System.nanoTime();
		
		thread1.start();
		thread2.start();
		thread3.start();
		//thread4.start();

		
		try {
			thread1.join();
			thread2.join();
			thread3.join();
			//thread4.join();
	      }
	      catch (InterruptedException e) { };
	      
	      
	      //System.out.println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	   /*   
	      long min ;
	      int j ;
	      int totItems = 0 ;
	      for (int h = 0; h < heads.length; h++) {
		      LeapNode head = heads[h];
		      min=-1;
		      totItems = 0;
		      j = 0;
			do 
			{
				//System.out.println("new node " + j + "\n");
				int k = 0;
				for (int i = 0 ; i < head.count ; i ++){
					//System.out.println(" Item Is " + head.data[i].value + "\n");
					k++;
					long val = (long) head.data[i].value; 
					if ( ( val > min))
					{
						min = val ;
					}
					else
					{
						System.out.println(" ERROR not sorted! ");
						return;
					}
					
					if ( val > head.high ){
						System.out.println(" ERROR not in range! ");
						return;
					}
				}
				//System.out.println("num of items " + k + "\n");
				totItems+=k;
				j++;
				head = head.getNext(0);
			}
			while (head!= null);
			
	      }
			System.out.println(" Total number of items before delete is " + totItems);*/
	     
	//	threadRQ.start();
		threadRem2.start();
	    threadRem3.start();
	    threadLook1.start();
	    try{
	   // 	threadRQ.join();
	    	threadRem2.join();
	    	threadRem3.join();
	    	threadLook1.join();
	    }
	    catch (InterruptedException e) { };
	      
	      long end = System.nanoTime();
	      /*
	      //System.out.println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	      for (int h = 0; h < heads.length; h++) {
			  LeapNode head = heads[h];
	       min = 0 ;
	       j = 0;
	       totItems = 0;
			do 
			{
				System.out.println("new node " + j + "\n");
				int k = 0;
				for (int i = 0 ; i < head.count ; i ++){
					//System.out.println(" Item Is " + head.data[i].value + "\n");
					k++;
					long val = (long) head.data[i].value; 
					if ( ( val > min))
					{
						min = val ;
					}
					else
					{
						System.out.println(" ERROR not sorted! ");
						return;
					}
					
					if ( val > head.high ){
						System.out.println(" ERROR not in range! ");
						return;
					}
				}
				System.out.println("num of items " + k + "\n");
				totItems+=k;
				j++;
				head = head.getNext(0);
			}
			while (head!= null);
			
			System.out.println(" Total number of items is " + totItems);
			
			System.out.println(" OK data is sorted! ");
	}*/
	      
	      System.out.println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	}
	
	public static void main(String[] args) {
			
		doTest();
	}

}

