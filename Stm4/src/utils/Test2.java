package utils;



import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;
import leapListReg.LeapNode;



public class Test2 {
	/*
	
	public static void fillThreadArray(LeapListDB  db,TestThread2[] threadArr, long[] arrRand, int size) {
		Random rand = new Random();
		int[] listToChoose = new int[3];
		int[] funcsToRun = new int[3];
		for (int i = 0; i < listToChoose.length; i++) {
			listToChoose[i] = rand.nextInt(4);
			funcsToRun[i] = rand.nextInt(4) + 4;
		}
		for (int i = 0; i < threadArr.length; i++) {
			threadArr[i] = new TestThread2(db, listToChoose , funcsToRun,arrRand,size);
		}
		
	}

	public static void doTest(){
		LeapListDB db =	new LeapListDB();
		TestThread2[] threads = new TestThread2[3];
		int arrSize = 1800;
		long[] arrRand = new long[arrSize];
		Random rand = new Random();
		for (int i = 0 ; i < arrSize ; i++){
			arrRand [i] = Math.abs(rand.nextInt()); 
		}
		fillThreadArray(db,threads,arrRand,arrSize/5);
		
		long start = System.nanoTime();
		
		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}

		
		try {
			for (int i = 0; i < threads.length; i++) {
				threads[i].join();
			}
	      }
	      catch (InterruptedException e) { };
	      
	      
	      //System.out.println(" Fine Grained Lock :  Time Elapsed : " + ((end - start) / 1000) / 1000 + "\n");
	      for (int t = 0; t < threads.length; t++) {
	
	    	TestThread2 tempThread =threads[t];
	      LeapNode head = tempThread.db.GetListByIndex(tempThread.lists[t]).GetHeadNode();
	      long min = -1 ;
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
				head = head.getNext(0);
			}
			while (head!= null);
			
			System.out.println(" Total number of items before delete is " + totItems);
	     
		threadRQ.start();
		threadRem2.start();
	    threadRem3.start();
	    threadLook1.start();
	    try{
	    	threadRQ.join();
	    	threadRem2.join();
	    	threadRem3.join();
	    	threadLook1.join();
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
				head = head.getNext(0);
			}
			while (head!= null);
			
			System.out.println(" Total number of items is " + totItems);
			
			System.out.println(" OK data is sorted! ");
	}
	}
	
	public static void main(String[] args) {
		
			doTest();
		
	}
*/
}

