package utils;

public class Test {

	public static void main(String[] args) {
		LeapSet key [] = new LeapSet[3];
		key [0] = new LeapSet(6,66);
		key [1] = new LeapSet(7,77);
		key [2] = new LeapSet(9,88);
		
		Trie tr = new Trie(key, 3);
		
		System.out.println (tr.trieFindVal(9));
	}

}
