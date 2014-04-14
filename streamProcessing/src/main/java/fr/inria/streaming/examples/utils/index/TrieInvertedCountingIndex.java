package fr.inria.streaming.examples.utils.index;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import fr.inria.streaming.examples.utils.index.TrieNode.ImproperLetterException;

public class TrieInvertedCountingIndex implements Serializable {

	private static final long serialVersionUID = 700018987546438391L;

	private static Logger logger = Logger.getLogger(TrieInvertedCountingIndex.class);
	
	private TrieNode root = new TrieNode();
	
	private Set<String> keys = new HashSet<String>();
	
	/**
	 * Returns null if there is no record in this index corresponding to the given word
	 * (i.e. this word hasn't been added to the index yet).
	 * @param word
	 */
	public Map<String,Integer> getEntriesForKey(String word) {
		
		if (word == null) {
			return null;
		}
		
		TrieNode currentNode = this.root;
		
		for (int i=0; i<word.length(); i++) {
			char c = word.charAt(i);
			if (!currentNode.hasDescendant(c)) {
				return null;
			}
			currentNode = currentNode.getDescendantFor(c);
		}
		
		return currentNode==root? null : currentNode.getIndexEntries();
	}
	
	public void increaseEntryForKey(String word, String docId, int num) {
		
		if (word==null || docId==null || num <= 0) {
			return;
		}
		
		if (word.length()==0 || docId.length()==0) {
			return;
		}
		
		TrieNode currentNode = this.root;
		char[] buf = new char[word.length()];
		
		for(int i=0; i<word.length(); i++) {
			char c = word.charAt(i);
			try {
				currentNode.addDescendantFor(c); // performs the check whether this add is unnecessary
				currentNode = currentNode.getDescendantFor(c);
				buf[i] = currentNode.getLetter();
				
			} catch (ImproperLetterException e) { 
				logger.warn("Attempted to add descendant for letter "+c+"to node "+currentNode.toString());
			} 
		}
		
		currentNode.increaseIndexEntryBy(docId, num);
		this.keys.add(new String(buf)); // the Set implementation makes sure we only have each element added once
		
	}
	
	public Set<String> getKeys() {
		return keys;
	}
}
