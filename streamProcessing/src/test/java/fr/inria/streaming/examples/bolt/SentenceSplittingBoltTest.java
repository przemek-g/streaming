package fr.inria.streaming.examples.bolt;

import java.util.Map;

import org.junit.Test;
import org.mockito.Matchers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static org.mockito.Mockito.*;

public class SentenceSplittingBoltTest {

	@Test
	public void shoudDeclareOutputFields() {
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		WordStemmingBolt bolt = new WordStemmingBolt();
		bolt.declareOutputFields(declarer);
		
		verify(declarer,times(1)).declare(Matchers.any(Fields.class));
	}
	
	@Test
	public void shouldEmitSomethingGivenProperSentenceTuple() {
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("this is a good sentence, my young Padawan");
		
		OutputCollector collector = mock(OutputCollector.class);
		
		SentenceSplittingBolt bolt = new SentenceSplittingBolt();
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		bolt.execute(tuple);
		verify(collector,times(8)).emit(Matchers.eq(tuple),Matchers.any(Values.class));
		
	}
	
	@Test
	public void shouldEmitSomethingGivenOneWordTuple() {
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("\t thisIsASentenceConsistingOfJustOneSimpleAlthoughVeryLongWord\n\t");
		
		OutputCollector collector = mock(OutputCollector.class);
		
		SentenceSplittingBolt bolt = new SentenceSplittingBolt();
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		bolt.execute(tuple);
		verify(collector,times(1)).emit(Matchers.eq(tuple),Matchers.any(Values.class));
		
	}
	
	@Test
	public void shouldEmitNothingGivenImproperSentenceTuple() {
		Tuple tuple = mock(Tuple.class);
		
		OutputCollector collector = mock(OutputCollector.class);
		
		SentenceSplittingBolt bolt = new SentenceSplittingBolt();
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);

		// first case
		when(tuple.getString(0)).thenReturn("");
		bolt.execute(tuple);
		
		// another case
		when(tuple.getString(0)).thenReturn(null);
		bolt.execute(tuple);

		// another case
		when(tuple.getString(0)).thenReturn("    ");
		bolt.execute(tuple);
		
		// in any of these cases, we should not emit anything
		verifyZeroInteractions(collector);
	}
}
