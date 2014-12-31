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
    static MyThread[] workers;
    static long startTime;
    static long endTime;
    static PaddedPrimitiveNonVolatile<Boolean> done = new PaddedPrimitiveNonVolatile<Boolean>(false);
    static PaddedPrimitive<Boolean> memFence = new PaddedPrimitive<Boolean>(false);
    static final int arrSize = 1000000;
    static final int initSize = 10000;
    
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
		int [] keyArr = new int [arrSize];
		int [] opArr = new int [arrSize];
		Random rand = new Random();
		
		System.out.println("filling Queues\n");
		//fill the key queue
		for (int i = 0; i < arrSize; i++) {
			keyArr[i] = (Math.abs(rand.nextInt()) % maxKey);
		}
		
		//fill the op queue
		for (int i = 0; i < arrSize; i++) {
			int index = (Math.abs(rand.nextInt()) % 100);
			if (index <= 59)
				opArr[i] = 0;
			else if (index <= 89)
				opArr[i] = 1;
			else if (index <=98)
				opArr[i] = 2;
			else
				opArr[i] = 3;
		}
		
		System.out.println("filling list");
		
		for (int i = 0; i < initSize; i++) {
			int tmp = keyArr[i];
			db.leapListUpdate(new LeapList[] {db.GetListByIndex(0)}, new long [] {tmp}, new Object [] {String.valueOf(tmp)}, 1);
		}
		
		System.out.println("initiating threads\n");
		
		workers = new MyThread[numOfThreads];
		threads = new Thread[numOfThreads];
		
		for (int i = 0; i < numOfThreads; i++) {
			int start;
			int range = (arrSize - initSize)/(numOfThreads +1);
			
			start = i*range + initSize;
			
			workers[i] = new MyThread(db, keyArr, opArr, keyRange, done, start, (start + range));
		}
		
		for (int i = 0; i < numOfThreads; i++) {
			threads[i] = new Thread(workers[i]);
		}
		
		System.out.println("running\n");
		startTime = System.currentTimeMillis();
		
		for (int i = 0; i < numOfThreads; i++) {
			threads[i].start();
		}
		
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e){;}
		
		done.value = true;
		memFence.value = true;
		
		try {
			for (int i = 0; i < numOfThreads; i++) {
				threads[i].join();
			}
		}catch (InterruptedException e){;}
		
		endTime = System.currentTimeMillis();
		
		System.out.println("finished running\n");
		int totCounter, lookCounter, rangeCounter, updateCounter, removeCounter;
		totCounter = lookCounter = rangeCounter = updateCounter = removeCounter = 0;
		
		for (int i = 0; i < numOfThreads; i++) {
			lookCounter += workers[i].lookCounter;
			rangeCounter += workers[i].rangeCounter;
			updateCounter += workers[i].updateCounter;
			removeCounter += workers[i].removeCounter;
			totCounter += workers[i].totCounter;
		}
		
		System.out.println("Number of lookups: " + lookCounter);
		System.out.println("\nNumber of range quaries: " + rangeCounter);
		System.out.println("\nNumber of updates: " + updateCounter);
		System.out.println("\nNumber of removes: " + removeCounter);
		System.out.println("Total number of successes: " + totCounter + "\nOps per seconds: " +  totCounter/((endTime - startTime)/1000) + "\n");
		
		return;
		
	}
}
