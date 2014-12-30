package utils;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import leapListReg.LeapList;
import leapListReg.LeapListDB;

public class Test1 {
	static int numOfThreads;
	static int keyRange;
	static int maxKey;
    static int seconds;
    static Thread[] threads;
    
	public static void main(String[] args) {
		if (args.length != 4){
			System.out.println("not enough arguments");
			return;
		}
		try {
			numOfThreads = Integer.parseInt(args[0]);
			maxKey = Integer.parseInt(args[1]);
			keyRange = Integer.parseInt(args[2]);
		    seconds = Integer.parseInt(args[3]);
		
		} catch(NumberFormatException e){
			System.out.println("arguments are not all ints");
			return;
		}
		
		LeapListDB db =	new LeapListDB();
		Queue<Integer> keyQ = new ConcurrentLinkedQueue<Integer>();
		Queue<Integer> opQ = new ConcurrentLinkedQueue<Integer>();
		Random rand = new Random();
		
		//fill the key queue
		for (int i = 0; i < 100000000; i++) {
			keyQ.add(Math.abs(rand.nextInt()) % maxKey);
		}
		
		//fill the op queue
		for (int i = 0; i < 100000000; i++) {
			int index = (Math.abs(rand.nextInt()) % 100);
			if (index <= 59)
				opQ.add(0);
			else if (index <= 89)
				opQ.add(1);
			else if (index <=98)
				opQ.add(2);
			else
				opQ.add(3);
		}
		
		for (int i = 0; i < 100000; i++) {
			int tmp = keyQ.remove();
			db.leapListUpdate(new LeapList[] {db.GetListByIndex(0)}, new long [] {tmp}, new Object [] {String.valueOf(tmp)}, 1);
		}
		threads = new Thread[numOfThreads];
		
		for (int i = 0; i < numOfThreads; i++) {
			threads[i] = new MyThread(db, keyQ, opQ, keyRange);
		}
		
		//TODO start clock and run the threads. 
	}
}
