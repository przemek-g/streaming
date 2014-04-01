package fr.inria.streaming.examples.utils.index;

import java.util.HashMap;
import java.util.Map;

public class TrieNode {

	/**
	 * The key-letter associated with this node
	 */
	private Character letter;
	
	
	/**
	 * The descendants of this node (each is associated with a key-letter).
	 */
	protected Map<Character, TrieNode> children = new HashMap<Character, TrieNode>();
	
	
	/**
	 * Tells if there is a word in the trie that ends with the letter associated with this node.
	 */
	protected boolean isTerminal = false;
	
	
	/**
	 * Holds counts of occurrences associated with ids of documents (titles, etc., hence the string keys).
	 */
	private Map<String,Integer> entries;
	
	private char getSmallLetterEquivalent(char c) throws ImproperLetterException {
		if (c>='A' && c<='Z') {
			return (char)(c+32);
		}
		if (!(c>='a' && c<='z')) {
			throw new ImproperLetterException(); 
		}
		
		return c;
	}

	// no need to initiate 'entries' in this constructor - we want it to remain null
	public TrieNode() {
		this.letter = ' ';
	}
	
	public TrieNode(char c) throws ImproperLetterException {
		this.letter = getSmallLetterEquivalent(c);
		this.entries = new HashMap<String, Integer>();
	}
	
	public void setTerminal(boolean shouldBeTerminal) {
		this.isTerminal = shouldBeTerminal;
	}
	
	public boolean isTerminal() {
		return this.isTerminal;
	}
	
	public Character getLetter() { 
		return this.letter; 
	}
	
	public boolean hasDescendant(char c) {
		try {
			return this.children.containsKey(getSmallLetterEquivalent(c));
		} catch (ImproperLetterException e) {
			return false;
		}
	}
	
	public TrieNode getDescendantFor(char c) {
		try {
			return this.children.get(getSmallLetterEquivalent(c));
		} catch (ImproperLetterException e) {
			return null;
		}
	}
	
	public void addDescendantFor(char keyLetter) throws ImproperLetterException {
		
		if (hasDescendant(keyLetter)) {
			return;
		}
		
		this.children.put(getSmallLetterEquivalent(keyLetter), new TrieNode(keyLetter));
	}
	
	public int getIndexEntryFor(String key) {
		return this.entries.get(key);
	}
	
	public Map<String, Integer> getIndexEntries() {
		return this.entries;
	}
	
	public void increaseIndexEntryBy(String key, int value) {
		if (this.entries.get(key)==null) {
			this.entries.put(key, 0);
		}
		
		int tmp = this.entries.get(key);
		int prev = tmp;
		tmp += value;
		
		// it's elegant to check for overflow cases, isn't it ;-)
		if (tmp < prev) {
			throw new IndexEntryOverflowException();
		}
		
		this.entries.put(key, tmp);
	}
	
	public String toString() {
		return "TrieNode(letter="+letter+")";
	}
	
	/* ---------------------------------------------------------------------- */
	public static class IndexEntryOverflowException extends RuntimeException {
		
		private static final long serialVersionUID = 8218011325989021708L;;
	}

	public static class ImproperLetterException extends Exception {

		private static final long serialVersionUID = 3688522991808586182L;
		
	}
}
