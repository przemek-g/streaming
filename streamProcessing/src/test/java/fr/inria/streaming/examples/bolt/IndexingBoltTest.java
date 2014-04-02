package fr.inria.streaming.examples.bolt;

import java.util.Map;

import org.junit.Test;

import fr.inria.streaming.examples.utils.IndexPersister;
import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import static org.mockito.Mockito.*;
import static org.fest.assertions.api.Assertions.assertThat;

public class IndexingBoltTest {

	@Test
	public void shouldIncreaseCountForWordByOne() {
		
		IndexingBolt bolt = new IndexingBolt();
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		// make a spy out of our bolt's indexing object in order to track its interactions
		TrieInvertedCountingIndex spyIndex = spy(bolt.getIndex());
		bolt.setIndex(spyIndex);
		
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("sample doc");
		when(tuple.getString(1)).thenReturn("example");
		when(tuple.getInteger(2)).thenThrow(IndexOutOfBoundsException.class);
		// the mock needs to indicate it's not a '__system'-'__tick' tuple
		when(tuple.getSourceComponent()).thenReturn("some_component_id");
		when(tuple.getSourceStreamId()).thenReturn("some_stream_id");
		
		bolt.execute(tuple);
		
		// these are pretty obvious ...
		verify(tuple,times(1)).getString(0);
		verify(tuple,times(1)).getString(1);
		verify(tuple,times(1)).getInteger(2);
		
		// this one's what matters
		verify(spyIndex,times(1)).increaseEntryForKey("example", "sample doc", 1);
		
		verify(collector,times(1)).ack(tuple);
	}
	
	@Test
	public void shouldIncreaseCountForWordByMoreThanOne() {
		
		IndexingBolt bolt = new IndexingBolt();
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		// spy the indexing object's interactions
		TrieInvertedCountingIndex spyIndex = spy(bolt.getIndex());
		bolt.setIndex(spyIndex);
		
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("sample doc");
		when(tuple.getString(1)).thenReturn("example");
		when(tuple.getInteger(2)).thenReturn(3);
		// the mock needs to indicate it's not a '__system'-'__tick' tuple
		when(tuple.getSourceComponent()).thenReturn("some_component_id");
		when(tuple.getSourceStreamId()).thenReturn("some_stream_id");
		
		bolt.execute(tuple);
		
		// these are pretty obvious ...
		verify(tuple,times(1)).getString(0);
		verify(tuple,times(1)).getString(1);
		verify(tuple,times(1)).getInteger(2);
		
		// this one's what matters
		verify(spyIndex,times(1)).increaseEntryForKey("example", "sample doc", 3);
		verify(collector,times(1)).ack(tuple);
		
	}
	
	@Test
	public void shouldDeclarePositiveFreqencyOfTicks() {
		IndexingBolt bolt = new IndexingBolt();
		Map<String, Object> configMap = bolt.getComponentConfiguration();
		
		assertThat(configMap).containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
		Integer tickFrequencySeconds = (Integer) configMap.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
		assertThat(tickFrequencySeconds).isGreaterThan(0);
	}
	
	@Test
	public void shouldPersistIndexWhenTickTupleReceived() {
		IndexingBolt bolt = new IndexingBolt();
		OutputCollector collector = mock(OutputCollector.class);
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		// spy the indexing object's interactions
		TrieInvertedCountingIndex spyIndex = spy(bolt.getIndex());
		bolt.setIndex(spyIndex);
		
		IndexPersister persister = mock(IndexPersister.class);
		doNothing().when(persister).persist(any(TrieInvertedCountingIndex.class));
		bolt.setIndexPersister(persister);
		
		Tuple tuple = mock(Tuple.class);
		// the mock needs to indicate it's a '__system'-'__tick' tuple
		when(tuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
		when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
		
		// run the method we want to test
		bolt.execute(tuple);
		
		// verify that the right things happened: we persist the index
		verify(bolt.getIndexPersister(), times(1)).persist(bolt.getIndex());
		// ... and do not perform anything on the OutputCollector
		verifyZeroInteractions(collector);
	}
}
