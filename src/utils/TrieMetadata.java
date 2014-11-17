package utils;

public class TrieMetadata {
	int prefix_length = 4;
	long prefix = 60;
	
	public TrieMetadata (){}
	
	public TrieMetadata (int prefix_length, long prefix){
		this.prefix = prefix;
		this.prefix_length = prefix_length;
	}
}
