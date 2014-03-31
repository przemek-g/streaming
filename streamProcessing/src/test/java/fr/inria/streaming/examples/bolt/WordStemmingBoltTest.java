package fr.inria.streaming.examples.bolt;

import static org.mockito.Mockito.*;

import java.util.Map;

import org.junit.Test;
import org.mockito.Matchers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordStemmingBoltTest {

//	private static class FieldsMatcher extends ArgumentMatcher<Fields> {
//		
//		private String patternToEq;
//		
//		public FieldsMatcher(String str) {
//			super();
//			this.patternToEq = str;
//		}
//		
//		@Override
//		public boolean matches(Object arg0) {
//			if (arg0 instanceof Fields) {
//				Fields fields = (Fields) arg0;
//				if (!fields.toString().equals(patternToEq)) {
//					return false;
//				}
//			}
//			
//			return true;
//		}
//	}
	
	@Test
	public void shoudDeclareOutputFields() {
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		WordStemmingBolt bolt = new WordStemmingBolt();
		bolt.declareOutputFields(declarer);
		
		verify(declarer,times(1)).declare(Matchers.any(Fields.class));
	}
	
	@Test
	public void shouldEmitSomethingIfCorrectTupleReceived() {
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("researchers");
		
		OutputCollector collector = mock(OutputCollector.class);
		
		WordStemmingBolt bolt = new WordStemmingBolt();
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		bolt.execute(tuple);
		verify(collector,times(1)).emit(Matchers.eq(tuple),Matchers.any(Values.class));
	}
	
	@Test
	public void shoudEmitNothingIfIncorrectTupleReceived() {
		Tuple tuple = mock(Tuple.class);
		when(tuple.getString(0)).thenReturn("some strange string");
		
		OutputCollector collector = mock(OutputCollector.class);
		
		WordStemmingBolt bolt = new WordStemmingBolt();
		bolt.prepare(mock(Map.class), mock(TopologyContext.class), collector);
		
		bolt.execute(tuple);
		
		verifyZeroInteractions(collector);
		
	}
}
