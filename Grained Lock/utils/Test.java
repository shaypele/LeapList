package utils;

import java.util.Random;


import leapListReg.LeapList;
import leapListReg.LeapListDB;
import leapListReg.LeapNode;

public class Test {

	public static void main(String[] args) {
		LeapListDB db =	new LeapListDB();
		LeapList list0 = db.GetListByIndex(0);
		int arrSize = 600;
		long[] arrRand = new long[arrSize];
		Random rand = new Random();
		for (int i = 0 ; i < arrSize ; i++){
			arrRand [i] = Math.abs(rand.nextLong()); 
		}
		TestThread thread1 = new TestThread( db , 4 , arrRand, 0 , arrSize / 3);
		TestThread thread2 = new TestThread( db , 4 , arrRand, arrSize / 3 ,arrSize * 2 /3);
		TestThread thread3 = new TestThread( db , 4 , arrRand, arrSize * 2 /3, arrSize );
		TestThread threadRem1 = new TestThread( db , 5 , arrRand, arrSize / 3 ,arrSize * 2 /3);
		TestThread threadRem2 = new TestThread( db , 5 , arrRand, arrSize * 2 /3, arrSize );
		long start = System.nanoTime();
		
		thread1.start();
		thread2.start();
		thread3.start();

		
		try {
			thread1.join();
			thread2.join();
			thread3.join();
	      }
	      catch (InterruptedException e) { };
	      
	      
	      //System.out.println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	      LeapNode head = list0.GetHeadNode();
	      long min = 0 ;
	      int j = 0;
	      int totItems = 0;
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
				head = head.next[0];
			}
			while (head!= null);
			
			System.out.println(" Total number of items before delete is " + totItems);
	     
	    threadRem1.start();
	    threadRem2.start();
	    try{
	    	threadRem1.join();
	    	threadRem2.join();
	    }
	    catch (InterruptedException e) { };
	      
	      long end = System.nanoTime();
	      
	      System.out.println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	       head = list0.GetHeadNode();
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
				head = head.next[0];
			}
			while (head!= null);
			
			System.out.println(" Total number of items is " + totItems);
			
			System.out.println(" OK data is sorted! ");
			
	}

}

