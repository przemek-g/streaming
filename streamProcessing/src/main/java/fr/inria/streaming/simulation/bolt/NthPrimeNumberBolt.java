package fr.inria.streaming.simulation.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.data.InvocationsCounter;
import fr.inria.streaming.simulation.util.NthPrimeNumberNaiveGenerator;

public class NthPrimeNumberBolt extends CountingBolt {

	private static final long serialVersionUID = -1087800281539238338L;
	private static Logger _logger = Logger.getLogger(NthPrimeNumberBolt.class);
	private static long _persistenceCounter = 0;
	private static InvocationsCounter _invocationsCounter = InvocationsCounter
			.getInstance(MostFrequentCharacterBolt.class.getName());
	
	public static final String INTENSITY_MODIFIER = "intensity-modifier";
	private static long intensityModifier = 1;
	
	public static long getExecutionsCount() {
		return _invocationsCounter.getCount();
	}

//	private NthPrimeNumberNaiveGenerator _primeGen = new NthPrimeNumberNaiveGenerator();
	
	public NthPrimeNumberBolt(long persistenceFrequencyHz, int tweetLength,
			int emissionFreq, String description, String throughputInfo) {
		super(persistenceFrequencyHz, tweetLength, emissionFreq, description,
				throughputInfo);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		if (stormConf.containsKey(INTENSITY_MODIFIER)) {
			try {
				intensityModifier = (Long)stormConf.get(INTENSITY_MODIFIER);
			} catch (ClassCastException e) {
				_logger.error("Exception while trying to get the INTENSITY_MODIFIER: "+e);
			}
		}
		
		super.prepare(stormConf, context, collector);
		
	}
	
	@Override
	protected Logger getLogger() {
		return _logger;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("n", "value"));
	}

	@Override
	protected Values process(Tuple tuple) {
		
		int l = tuple.getValueByField("text").toString().length();
		_logger.info("Processing a(n)"+l+" characters long tweet");
		if (intensityModifier>1) {
			l *= intensityModifier;
		}
		_logger.info("Generating "+l+"th prime number...");
		long prime = new NthPrimeNumberNaiveGenerator().generateNthPrime(l);
		
		return new Values(l,prime);
	}

	@Override
	public long getPersistenceCount() {
		return _persistenceCounter;
	}

	@Override
	protected void incrementPersistenceCount() {
		_persistenceCounter++;
	}

	@Override
	protected InvocationsCounter getInvocationsCounter() {
		return _invocationsCounter;
	}
}
