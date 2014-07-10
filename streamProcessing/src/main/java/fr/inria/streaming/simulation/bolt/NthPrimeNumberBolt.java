package fr.inria.streaming.simulation.bolt;

import org.apache.log4j.Logger;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.data.InvocationsCounter;

public class NthPrimeNumberBolt extends CountingBolt {

	private static final long serialVersionUID = -1087800281539238338L;
	private static Logger logger = Logger.getLogger(NthPrimeNumberBolt.class);
	private static long _persistenceCounter = 0;
	private static InvocationsCounter _invocationsCounter = InvocationsCounter
			.getInstance(MostFrequentCharacterBolt.class.getName());

	protected NthPrimeNumberBolt(long persistenceFrequencyHz, int tweetLength,
			int emissionFreq, String description, String throughputInfo) {
		super(persistenceFrequencyHz, tweetLength, emissionFreq, description,
				throughputInfo);
	}

	@Override
	public void execute(Tuple tuple) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("n", "value"));
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	protected Values process(Tuple tuple) {
//		String text = tuple.getValueByField("text").toString();
//		for (int i = 0; i < text.length(); i++) {
//			_charsCounter.count(text.charAt(i));
//		}
//
//		return new Values(_charsCounter.getMaxResultCharacter(),
//				_charsCounter.getMaxResultCount());
		
		return null;

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
