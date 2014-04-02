package fr.inria.streaming.examples.utils;

import static org.mockito.Mockito.*;

import java.io.File;

import org.junit.Test;

import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;

public class TextFileIndexPersisterTest {

	@Test
	public void testWithNonExistentFile() {
		String fileName = "someNonExistentFile.txt";
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		
		TextFileIndexPersister persister = new TextFileIndexPersister(fileName);
		
		TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
		index.increaseEntryForKey("hey", "1", 1);
		index.increaseEntryForKey("hey", "2", 2);
		index.increaseEntryForKey("hey", "3", 3);
		
		index.increaseEntryForKey("heythere", "3", 1);
		index.increaseEntryForKey("heythere", "2", 2);
		index.increaseEntryForKey("heythere", "1", 3);

		index = spy(index);
		
		persister.persist(index);
		
		verify(index).getKeys();
		verify(index).getEntriesForKey("hey");
		verify(index).getEntriesForKey("heythere");
		
	}
}
