package fr.inria.streaming.simulation.spout;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Map;

import org.mockito.Mockito;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.util.TextContentSource;

public class FrequencyEmissionSpoutTest {

	private static final long ONE_SECONDS_NANOS = 1000000000;
	private static char[] textContent;
	private static Logger logger = Logger.getLogger(FrequencyEmissionSpoutTest.class);

	private FrequencyEmissionSpout spout;
	private TextContentSource textContentSource;
	private SpoutOutputCollector spoutOutputCollector;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	
	
	
	@BeforeClass
	public static void setUpBeforeClass() {
		textContent = new char[140];
		for (int i=0; i<textContent.length; i++) {
			textContent[i] = 'a';
		}
	}
	
	@Test
	public void testEmissions() {
		int secondsToSleep = 5;
		
		textContentSource = Mockito.mock(TextContentSource.class);
		Mockito.when(textContentSource.getTextContent()).thenReturn(textContent);
		
		spout = new FrequencyEmissionSpout(ONE_SECONDS_NANOS, textContentSource);
		spoutOutputCollector = Mockito.mock(SpoutOutputCollector.class);
		Mockito.when(spoutOutputCollector.emit(Mockito.anyListOf(Object.class))).thenReturn(new ArrayList<Integer>());
		
		outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		
		spout.declareOutputFields(outputFieldsDeclarer);
		Mockito.verify(outputFieldsDeclarer).declare(Mockito.any(Fields.class));
		
		spout.open(Mockito.mock(Map.class), Mockito.mock(TopologyContext.class), spoutOutputCollector);
		logger.info("Opened FrequencyEmissionSpout, now going to sleep for "+String.valueOf(secondsToSleep)+" seconds");
		try {
			Thread.sleep(secondsToSleep*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail();
		}
		spout.close();
		Mockito.verify(spoutOutputCollector,Mockito.atLeast(secondsToSleep)).emit(Mockito.any(Values.class));
		logger.info("Going to sleep for some more seconds to verify there are no more interactions with the spout's SpoutOutputCollector");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail();
		}
		Mockito.verifyNoMoreInteractions(spoutOutputCollector);
		
	}

}
