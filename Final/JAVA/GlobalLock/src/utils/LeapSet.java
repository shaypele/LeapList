package utils;
/*
 * LeapSet class holds long key and Object value.
 * each node has an array of LeapSet that represent all the keys and values in the node.
 */


public class LeapSet {
	public long key;
	public Object value;
	
	public LeapSet (long key, Object value){
		this.key = key;
		this.value = value;
	}
}
