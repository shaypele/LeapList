package utils;

import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;
import leapListReg.LeapNode;

public class Test {

	public static void main(String[] args) {
		LeapListDB db =	new LeapListDB();
		LeapList list0 = db.GetListByIndex(0);
		
		long[] arrRand = new long[300];
		Random rand = new Random();
		for (int i = 0 ; i < 300 ; i++){
			arrRand [i] = Math.abs(rand.nextLong()); 
		}
		TestThread thread1 = new TestThread( db , 4 , arrRand, 0 , 100);
		TestThread thread2 = new TestThread( db , 4 , arrRand, 100 , 200);
		TestThread thread3 = new TestThread( db , 4 , arrRand, 200 , 300);
		
		thread1.start();
		thread2.start();
		thread3.start();

		
		try {
			thread1.join();
			thread2.join();
			thread3.join();
	      }
	      catch (InterruptedException e) { };

	      LeapNode head = list0.GetHeadNode();
	      long min = 0 ;
	      int j = 0;
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
				j++;
				head = head.next[0];
			}
			while (head!= null);
			
			System.out.println(" OK data is sorted! ");
			
	}

}

