package utils;

public class Test {

	public static void main(String[] args) {
		int key [] = new int[3];
		key [0] = 0;
		key [1] = 1;
		key [2] = 4;
		short value = (short)5;
		
		Trie tr = new Trie(null, key, 3);
		
		System.out.println (tr.trieFindVal(key[2]));
	}

}
