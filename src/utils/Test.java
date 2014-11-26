package utils;

import leapListReg.LeapList;
import leapListReg.LeapListDB;

public class Test {

	public static void main(String[] args) {
		LeapListDB db =	new LeapListDB();
		LeapList list0 = db.GetListByIndex(0);
		db.leapListUpdate(new LeapList[] {list0,list0,list0,list0,list0}, new long[]{1, 4,7,9,20}, new Object[]{"First", "2nd","3rd","4th","5th"},5);
		
	}

}
