package fr.inria.streaming.examples.spout;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import fr.inria.streaming.examples.utils.TextFileReader;
import static org.mockito.Mockito.*;

public class TextContentSpoutTest {

	@Test
	public void shouldDeclareSomeFields() {
		TextContentSpout spout = new TextContentSpout(new TextFileReader("nonExistentFile"));
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		spout.declareOutputFields(declarer);
		verify(declarer,times(1)).declare(any(Fields.class));
	}
	
	@Test
	public void testWithoutFileToRead() {
		TextContentSpout spout = new TextContentSpout(new TextFileReader("nonExistentFile"));
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		
		spout.open(mock(Map.class), mock(TopologyContext.class), collector);
		spout.nextTuple();
		
		verifyZeroInteractions(collector);
		
	}
	
	@Test
	public void testWithEmptyFileToRead() {
		TextContentSpout spout = new TextContentSpout(new TextFileReader("empty-file.txt"));
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		
		spout.open(mock(Map.class), mock(TopologyContext.class), collector);
		spout.nextTuple();
		
		verifyZeroInteractions(collector);
	}
	
	@Test
	public void testWithSeveralLinesToRead() {
		TextContentSpout spout = new TextContentSpout(new TextFileReader("five-lines-file.txt"));
		
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		when(collector.emit(any(Values.class))).thenReturn(new ArrayList<Integer>());
		
		spout.open(mock(Map.class), mock(TopologyContext.class), collector);

		for (int i=0; i<10; i++) {
			spout.nextTuple();
		}

		verify(collector,times(5)).emit(any(Values.class)); // only going to read five lines from the file
	}
}
