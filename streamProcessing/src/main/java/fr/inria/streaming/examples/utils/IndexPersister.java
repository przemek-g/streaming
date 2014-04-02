package fr.inria.streaming.examples.utils;

import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;

public interface IndexPersister {
	
	void persist(TrieInvertedCountingIndex index);

}
