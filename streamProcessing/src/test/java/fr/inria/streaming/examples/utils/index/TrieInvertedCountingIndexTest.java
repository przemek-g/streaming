package fr.inria.streaming.examples.utils.index;

import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;

public class TrieInvertedCountingIndexTest {

	@Test
	public void testCreation() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		Map<String, Integer> result = index.getEntriesForKey(null);
		assertNull(result);
		
		result = index.getEntriesForKey("");
		assertNull(result);
		
		result = index.getEntriesForKey(" ");
		assertNull(result);
		
		result = index.getEntriesForKey("ala");
		assertNull(result);
		
		result = index.getEntriesForKey("ala ma kota");
		assertNull(result);
		
	}
	
	@Test
	public void testAddingInvalidWords() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		
		index.increaseEntryForKey(null, "1", 1);
		Map<String, Integer> result = index.getEntriesForKey(null);
		assertNull(result);
		
		index.increaseEntryForKey("something", null, 1);
		result = index.getEntriesForKey("something");
		assertNull(result);
		
		index.increaseEntryForKey("", "1", 1);
		result = index.getEntriesForKey("");
		assertNull(result);
		
		index.increaseEntryForKey("something", "", 1);
		result = index.getEntriesForKey("something");
		assertNull(result);

	}
	
	@Test
	public void testAddingValidWords() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		
		index.increaseEntryForKey("Hey", "1", 1);
		Map<String, Integer> result = index.getEntriesForKey("Hey");
		assertNotNull(result);
		assertEquals(1,result.size());
		assertEquals(1,result.get("1").intValue());
		
		// add to the same document for the same key
		index.increaseEntryForKey("Hey", "1", 2);
		assertEquals(1,result.size());
		assertEquals(3,result.get("1").intValue());
		
		// add to the same document for the same key
		index.increaseEntryForKey("Hey", "2", 1);
		assertEquals(2,result.size());
		assertEquals(1,result.get("2").intValue());
		
		// another document; add max integer
		index.increaseEntryForKey("Hey", "3", Integer.MAX_VALUE);
		assertEquals(3,result.size());
		assertEquals(Integer.MAX_VALUE,result.get("3").intValue());
		
		// try another word
		index.increaseEntryForKey("HeyThere", "1", 2);
		assertEquals(3,result.size()); // make sure it hasn't affected the previous key
		result = index.getEntriesForKey("HeyThere");
		assertEquals(1,result.size());
		assertEquals(2,result.get("1").intValue());
		
		// try another word
		index.increaseEntryForKey("HeyThere", "2", 2);
		assertEquals(2,result.size());
		assertEquals(2,result.get("2").intValue());
	}
	
	@Test(expected=TrieNode.IndexEntryOverflowException.class)
	public void shouldThrowExceptionWhenAddingOverflowingNumber() {
		
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		index.increaseEntryForKey("Hey", "1", 1);
		index.increaseEntryForKey("Hey", "1", Integer.MAX_VALUE);
	}
	
	@Test(expected=TrieNode.IndexEntryOverflowException.class)
	public void shouldThrowExceptionWhenAddingMaximalNumberTwice() {
		
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		index.increaseEntryForKey("Hey", "1", Integer.MAX_VALUE);
		Map<String, Integer> result = index.getEntriesForKey("Hey");
		assertEquals(1,result.size());
		assertEquals(Integer.MAX_VALUE,result.get("1").intValue());
		
		// add max_value to max_value
		index.increaseEntryForKey("Hey", "1", Integer.MAX_VALUE);
	}
}
