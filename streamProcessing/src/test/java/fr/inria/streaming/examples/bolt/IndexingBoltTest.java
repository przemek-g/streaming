package fr.inria.streaming.examples.bolt;

import org.junit.Test;

import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;
import backtype.storm.tuple.Tuple;
import static org.mockito.Mockito.*;

public class IndexingBoltTest {

	@Test
	public void shouldIncreaseCountForWordByOne() {
		
		IndexingBolt bolt = new IndexingBolt();
		TrieInvertedCountingIndex spyIndex = spy(bolt.getIndex());
		bolt.setIndex(spyIndex);
		
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("sample doc");
		when(tuple.getString(1)).thenReturn("example");
		when(tuple.getInteger(2)).thenThrow(IndexOutOfBoundsException.class);
		
		bolt.execute(tuple);
		
		// these are pretty obvious ...
		verify(tuple,times(1)).getString(0);
		verify(tuple,times(1)).getString(1);
		verify(tuple,times(1)).getInteger(2);
		
		// this one's what matters
		verify(spyIndex,times(1)).increaseEntryForKey("example", "sample doc", 1);
	}
	
	@Test
	public void shouldIncreaseCountForWordByMoreThanOne() {
		
		IndexingBolt bolt = new IndexingBolt();
		TrieInvertedCountingIndex spyIndex = spy(bolt.getIndex());
		bolt.setIndex(spyIndex);
		
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("sample doc");
		when(tuple.getString(1)).thenReturn("example");
		when(tuple.getInteger(2)).thenReturn(3);
		
		bolt.execute(tuple);
		
		// these are pretty obvious ...
		verify(tuple,times(1)).getString(0);
		verify(tuple,times(1)).getString(1);
		verify(tuple,times(1)).getInteger(2);
		
		// this one's what matters
		verify(spyIndex,times(1)).increaseEntryForKey("example", "sample doc", 3);
		
	}
}
