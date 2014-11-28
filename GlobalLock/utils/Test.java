package utils;

import leapListReg.LeapList;
import leapListReg.LeapListDB;
import leapListReg.LeapNode;

public class Test {

	public static void main(String[] args) {
		LeapListDB db =	new LeapListDB();
		LeapList list0 = db.GetListByIndex(0);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{3, }, new Object[]{"1st"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{5, }, new Object[]{"3rd"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{90, }, new Object[]{"5th"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{7}, new Object[]{"4th"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{4, }, new Object[]{"2nd"},1);
		db.leapListUpdate(new LeapList[] {list0}, new long[]{7, }, new Object[]{"I'm am 7"},1);
		LeapNode head = list0.GetHeadNode();
		do 
		{
			System.out.println("new node \n");
			for (int i = 0 ; i < head.count ; i ++){
				System.out.println(" Item Is " + head.data[i].value + "\n"); 
			}
			head = head.next[0];
		}
		while (head!= null);
		
		System.out.println(" Rangen ");
		Object[] arr =  list0.RangeQuery(7, 90);
		for (Object obj : arr){
			System.out.println(" Item Is " + obj.toString() + "\n");
		}
		
		System.out.println(" Remove ");
		//db.leapListRemove(new LeapList[] {list0}, new long[]{90, },1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{5, },1);
		/*db.leapListRemove(new LeapList[] {list0}, new long[]{7},1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{4, },1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{3, },1);
		db.leapListRemove(new LeapList[] {list0}, new long[]{5, },1);
		*/
		head = list0.GetHeadNode();
		do 
		{
			System.out.println("new node \n");
			for (int i = 0 ; i < head.count ; i ++){
				System.out.println(" Item Is " + head.data[i].value + "\n"); 
			}
			head = head.next[0];
		}
		while (head!= null);
		
		System.out.println("LOOKUP: \n");
		Object obji = list0.lookUp(7);
		String strLookup = " I'm null";
		if ( obji != null )
		{
			strLookup = obji.toString();
		}
		System.out.println( "Result IS " +  strLookup);
		
		
		System.out.println("XXXXXX");
	}

}
