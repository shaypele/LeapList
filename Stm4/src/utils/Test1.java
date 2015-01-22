package utils;

import java.util.Random;

import leapListReg.LeapList;
import leapListReg.LeapListDB;

public class Test1 {
	static int numOfThreads;
	static int keyRange;
	static int maxKey;
	static int rangeProp;
	static int insertProp;
	static int lookUpProp;
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
    
	public static void main(String[] args) {
		if (args.length != 8){
			System.out.println("not enough arguments: <number of Threads> <max key> <key Range> <seconds> <number of lists> <LookUp Proportion> <Range Proportion> <Insert Proportion>");
			return;
		}
		try {
			numOfThreads = Integer.parseInt(args[0]);
			maxKey = Integer.parseInt(args[1]);
			keyRange = Integer.parseInt(args[2]);
		    seconds = Integer.parseInt(args[3]);
		    numberOfLists = Integer.parseInt(args[4]);
		    lookUpProp = Integer.parseInt(args[5]);
		    rangeProp= Integer.parseInt(args[6]);
		    insertProp = Integer.parseInt(args[7]);
		
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
		
		System.out.println("filling Queues\n");
		//fill the key queue
		for (int i = 0; i < arrSize; i++) {
			keyArr[i] = (Math.abs(rand.nextInt()) % maxKey);
		}
		
		//fill the op queue
		for (int i = 0; i < arrSize; i++) {
			int index = (Math.abs(rand.nextInt()) % 100);
			if (index <= lookUpProp -1)
				opArr[i] = 0;
			else if (index <= lookUpProp + rangeProp -1)
				opArr[i] = 1;
			else if (index <=insertProp + lookUpProp + rangeProp -1)
				opArr[i] = 2;
			else
				opArr[i] = 3;
		}
		
		System.out.println("filling list");
		
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
		
		System.out.println("initiating threads\n");
		LeapListDB.returnOnSplit = true;
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
		System.out.println("\nTotal number of successes: " + totCounter + "\nOps per seconds: " +  (double)totCounter/((endTime - startTime)/1000) + "\n");
		
		return;
		
	}
}
