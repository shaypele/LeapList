package utils;


public class TrieMetadata {
	byte prefix_length ;
	long prefix ;
	
	public TrieMetadata (){}
	
	public TrieMetadata (byte prefix_length, long prefix){
		this.prefix = prefix; 
		this.prefix_length = prefix_length;
	}
}
