package fr.inria.streaming.simulation.spout;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.util.FakeTweetContentSource;
import fr.inria.streaming.simulation.util.ITextContentSource;

public class FrequencyEmissionSpoutTest {

	private static char[] textContent;
	private static Logger logger = Logger.getLogger(FrequencyEmissionSpoutTest.class);

	private FrequencyEmissionSpout spout;
	private ITextContentSource textContentSource;
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
	public void testDifferentEmissionFrequencies() {
		for (int i=1; i<=3;i++) {
			for (int j=1; j<=5; j++) {
				testEmissions(j,i);
			}
		}
		
//		testEmissions(5,2);
	}
	
	private void testEmissions(int frequencyHertz, int secondsToSleep) {
		
		logger.info(new StringBuilder(
				"Testing FrequencyEmissionSpout for frequency: ")
				.append(frequencyHertz).append(", secondsToSleep: ")
				.append(secondsToSleep).toString());
		
		textContentSource = Mockito.mock(ITextContentSource.class);
		Mockito.when(textContentSource.getTextContent()).thenReturn(textContent);
		
		spout = new FrequencyEmissionSpout(frequencyHertz, FakeTweetContentSource.getTweetLength(), "testing FrequencyEmissionSpout", "someThroughput", textContentSource);
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
		
		// verify the number of interactions
		Mockito.verify(spoutOutputCollector,Mockito.atLeast(secondsToSleep*frequencyHertz)).emit(Mockito.any(Values.class));
		Mockito.verify(textContentSource,Mockito.atLeast(secondsToSleep*frequencyHertz)).getTextContent();
		
		logger.info("Going to sleep for some more seconds to verify there are no more interactions with the spout's SpoutOutputCollector");
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail();
		}
		Mockito.verifyNoMoreInteractions(spoutOutputCollector);
		
	}

}
