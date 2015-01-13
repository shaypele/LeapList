package utils;

import java.util.ArrayList;
import java.util.HashSet;
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
    static final int arrSize = 10000000;
    static final int initSize = arrSize/1000; 
    static int numberOfLists;
    

  
    
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		
		
		if (args.length != 5){
			System.out.println("not enough arguments: <number of Threads> <max key> <key Range> <seconds> <number of lists>");
			return;
		}
		try {
			numOfThreads = Integer.parseInt(args[0]);
			maxKey = Integer.parseInt(args[1]);
			keyRange = Integer.parseInt(args[2]);
		    seconds = Integer.parseInt(args[3]);
		    numberOfLists = Integer.parseInt(args[4]);
		
		} catch(NumberFormatException e){
			System.out.println("arguments are not all ints");
			return;
		}
		
		if (numOfThreads < 1){
			System.out.println("Must have at least one thread");
			return;
		}
		
		if (maxKey < 1){
			System.out.println ("must have more than one key");
			return;
		}
		
		if (keyRange < 1){
			System.out.println("key range must be at least 1");
			return;
		}
		
		if (seconds < 1){
			System.out.println("Must work for more than 1 second");
			return;
		}
		
		if (numberOfLists < 1){
			System.out.println("Must have at least one list");
			return;
		}
		
		LeapListDB db =	new LeapListDB(numberOfLists);
		int [] keyArr = new int [arrSize];
		int [] opArr = new int [arrSize];
		Random rand = new Random();
		
		//system.out.println("filling Queues\n");
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
		
		//system.out.println("filling list");
		
		LeapList[] leapArr = new LeapList[numberOfLists];
		long [] keys = new long [numberOfLists];
		Object [] vals = new Object [numberOfLists];
		
		for (int i = 0; i < initSize; i+=numberOfLists) {	
			for (int j = 0; j < numberOfLists; j++) {
				leapArr[j] = db.GetListByIndex(j);
				keys [j] = keyArr[i + j];
				vals [j] = String.valueOf(keyArr[i+j]);
			}
			db.leapListUpdate(leapArr, keys, vals, numberOfLists);
		}
		
		//system.out.println("initiating threads\n");
		
		workers = new MyThread[numOfThreads];
		threads = new Thread[numOfThreads];
		
		for (int i = 0; i < numOfThreads; i++) {
			int start;
			int range = (arrSize - initSize)/(numOfThreads +1);
			
			start = i*range + initSize;
			
			workers[i] = new MyThread(db, keyArr, opArr, keyRange, done, start, (start + range), numberOfLists);
		}
		
		for (int i = 0; i < numOfThreads; i++) {
			threads[i] = new Thread(workers[i]);
		}
		
		//system.out.println("running\n");
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
				threads[i].join(4000);
				if (threads[i].isAlive())
				{
					System.out.println("Stuck!");
					for (int j = 0; j < numOfThreads; j++) 
					{
						if (threads[j].isAlive())
						{
							threads[j].stop();
						}
					}
					return;
				}
			}
		}catch (InterruptedException e){;}
		
		endTime = System.currentTimeMillis();
		
		//system.out.println("finished running\n");
		int totCounter, lookCounter, rangeCounter, updateCounter, removeCounter;
		totCounter = lookCounter = rangeCounter = updateCounter = removeCounter = 0;
		
		for (int i = 0; i < numOfThreads; i++) {
			lookCounter += workers[i].lookCounter;
			rangeCounter += workers[i].rangeCounter;
			updateCounter += workers[i].updateCounter;
			removeCounter += workers[i].removeCounter;
			totCounter += workers[i].totCounter;
		}
		
		//system.out.println("Number of lookups: " + lookCounter);
		//system.out.println("\nNumber of range quaries: " + rangeCounter);
		//system.out.println("\nNumber of updates: " + updateCounter);
		//system.out.println("\nNumber of removes: " + removeCounter);
		//system.out.println("\nTotal number of successes: " + totCounter + "\nOps per seconds: " +  totCounter/((endTime - startTime)/1000) + "\n");
		System.out.println(totCounter/((endTime - startTime)/1000) + "\n");
		
		return;
		
	}
}
