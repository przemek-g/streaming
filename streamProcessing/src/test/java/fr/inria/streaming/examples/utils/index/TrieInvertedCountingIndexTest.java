package fr.inria.streaming.examples.utils.index;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.fest.assertions.api.Assertions.*;

public class TrieInvertedCountingIndexTest {

	@Test
	public void testCreation() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		Map<String, Integer> result = index.getEntriesForKey(null);
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
		result = index.getEntriesForKey("");
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
		result = index.getEntriesForKey(" ");
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
		result = index.getEntriesForKey("ala");
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
		result = index.getEntriesForKey("ala ma kota");
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
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
		assertThat(index.getKeys()).hasSize(0);
		
		index.increaseEntryForKey("", "1", 1);
		result = index.getEntriesForKey("");
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
		index.increaseEntryForKey("something", "", 1);
		result = index.getEntriesForKey("something");
		assertNull(result);
		assertThat(index.getKeys()).hasSize(0);
		
		index.increaseEntryForKey("word", "1", -5);
		assertThat(index.getEntriesForKey("word")).isNull();
		assertThat(index.getKeys()).hasSize(0);

	}
	
	@Test
	public void testAddingValidWords() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		
		// --- FIRST KEY -----------------------
		index.increaseEntryForKey("Hey", "1", 1);
		Map<String, Integer> result = index.getEntriesForKey("Hey");
		assertNotNull(result);
		assertThat(result).hasSize(1);
		assertEquals(1,result.get("1").intValue());
		assertThat(index.getKeys()).hasSize(1);
		
		// add to the same document for the same key
		index.increaseEntryForKey("Hey", "1", 2);
		assertEquals(1,result.size());
		assertEquals(3,result.get("1").intValue());
		assertThat(index.getKeys()).hasSize(1);
		
		// add to another document for the same key
		index.increaseEntryForKey("Hey", "2", 1);
		assertEquals(2,result.size());
		assertEquals(1,result.get("2").intValue());
		assertThat(index.getKeys()).hasSize(1);
		
		// another document; add max integer
		index.increaseEntryForKey("Hey", "3", Integer.MAX_VALUE);
		assertEquals(3,result.size());
		assertEquals(Integer.MAX_VALUE,result.get("3").intValue());
		assertThat(index.getKeys()).hasSize(1);
		
		// --- try a SUB-WORD KEY --------------
		index.increaseEntryForKey("He", "1", 1);
		result = index.getEntriesForKey("He");
		assertThat(result).hasSize(1);
		assertThat(result.get("1")).isEqualTo(1);
		assertThat(index.getKeys()).hasSize(2);
		
		index.increaseEntryForKey("He", "1", 10);
		assertThat(result).hasSize(1);
		assertThat(result.get("1")).isEqualTo(11);
		assertThat(index.getKeys()).hasSize(2);
		
		index.increaseEntryForKey("He", "2", 10);
		assertThat(result).hasSize(2);
		assertThat(result.get("2")).isEqualTo(10);
		assertThat(index.getKeys()).hasSize(2);
		
		// make sure it hasn't affected the previous key
		assertThat(index.getEntriesForKey("Hey")).hasSize(3);
		
		// try a super-word
		index.increaseEntryForKey("HeyThere", "1", 2);
		result = index.getEntriesForKey("HeyThere");
		assertThat(result).hasSize(1);
		assertThat(result.get("1")).isEqualTo(2);
		assertThat(index.getKeys()).hasSize(3);
		
		index.increaseEntryForKey("HeyThere", "2", 2);
		assertThat(result).hasSize(2);
		assertThat(result.get("2")).isEqualTo(2);
		assertThat(index.getKeys()).hasSize(3);
		
		index.increaseEntryForKey("HeyThere", "1", 6);
		assertThat(result).hasSize(2);
		assertThat(result.get("1")).isEqualTo(8);
		assertThat(index.getKeys()).hasSize(3);
		
		assertThat(index.getEntriesForKey("He")).hasSize(2);
		assertThat(index.getEntriesForKey("Hey")).hasSize(3);
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
	
	@Test
	public void testForKeysCorrectness() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		assertThat(index.getKeys()).hasSize(0);
		
		index.increaseEntryForKey("ba", "1", 1);
		assertThat(index.getKeys()).hasSize(1);
		index.increaseEntryForKey("ba", "1", 1);
		assertThat(index.getKeys()).hasSize(1);
		
		// add to a super-word for the same doc
		index.increaseEntryForKey("bana","1", 4);
		assertThat(index.getKeys()).hasSize(2);
		
		index.increaseEntryForKey("banana", "docID", 66);
		assertThat(index.getKeys()).hasSize(3);
		
		index.increaseEntryForKey("other", "docId", 55);
		assertThat(index.getKeys()).hasSize(4);
		
		Set<String> expectedKeys = new HashSet<String>();
		expectedKeys.add("ba");
		expectedKeys.add("bana");
		expectedKeys.add("banana");
		expectedKeys.add("banana");
		assertThat(index.getKeys()).containsAll(expectedKeys);
		
	}

	@Test
	public void testAddingSameWordsWithDifferentCapitalLetters() {
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		
		index.increaseEntryForKey("hakunamatata", "1", 1);
		index.increaseEntryForKey("hakunAmataTa", "1", 2);
		assertThat(index.getKeys()).hasSize(1);
		assertThat(index.getEntriesForKey("haKunAMataTA")).hasSize(1);
		
		index.increaseEntryForKey("HAKUNAMATATA", "2", 3);
		assertThat(index.getKeys()).hasSize(1);
		assertThat(index.getEntriesForKey("HaKuNaMaTaTa")).hasSize(2);
	}
}
