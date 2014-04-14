package fr.inria.streaming.examples.spout;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import fr.inria.streaming.examples.spout.TextContentSpout.WrongFileNameException;

public class TextContentSpoutTest {

	@Test
	public void shouldDeclareSomeFields() throws Exception {
		TextContentSpout spout = new TextContentSpout("nonExistentFile");
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		spout.declareOutputFields(declarer);
		verify(declarer,times(1)).declare(any(Fields.class));
	}
	
	@Test
	public void testWithoutFileToRead() throws Exception {
		TextContentSpout spout = new TextContentSpout("nonExistentFile");
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		
		spout.open(mock(Map.class), mock(TopologyContext.class), collector);
		spout.nextTuple();
		
		verifyZeroInteractions(collector);
		
	}
	
	@Test(expected=WrongFileNameException.class)
	public void shouldThrowExceptionWithNullFileName() throws Exception {
		new TextContentSpout(null);
	}
	
	@Test(expected=WrongFileNameException.class)
	public void shouldThrowExceptionWithEmptyFileName() throws Exception {
		new TextContentSpout("");
	}
	
	@Test
	public void testWithEmptyFileToRead() throws Exception {
		TextContentSpout spout = new TextContentSpout("empty-file.txt");
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		
		spout.open(mock(Map.class), mock(TopologyContext.class), collector);
		spout.nextTuple();
		
		verifyZeroInteractions(collector);
	}
	
	@Test
	public void testWithSeveralLinesToRead() throws Exception {
		TextContentSpout spout = new TextContentSpout("five-lines-file.txt");
		
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		when(collector.emit(any(Values.class))).thenReturn(new ArrayList<Integer>());
		
		spout.open(mock(Map.class), mock(TopologyContext.class), collector);

		for (int i=0; i<10; i++) {
			spout.nextTuple();
		}

		verify(collector,times(5)).emit(any(Values.class)); // only going to read five lines from the file
	}
}
