package fr.inria.streaming.simulation.bolt;

import org.apache.log4j.Logger;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.data.InvocationsCounter;
import fr.inria.streaming.simulation.util.CharactersCounter;

public class MostFrequentCharacterBolt extends CountingBolt {

	// --- static ---
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger
			.getLogger(MostFrequentCharacterBolt.class);
	private static long _persistenceCounter = 0;
	private static InvocationsCounter _invocationsCounter = InvocationsCounter
			.getInstance(MostFrequentCharacterBolt.class.getName());

	public static long getExecutionsCount() {
		return _invocationsCounter.getCount();
	}

	// --- instance ---
	private CharactersCounter _charsCounter = new CharactersCounter();

	// private ICountPersister _persister;
	// private long _persistencePeriodNanos;
	// private String _description;
	// private String _elementType = "BOLT";
	// private String _networkBandwidth;
	// private int _tweetLength;
	// private int _emissionFrequency;

	public MostFrequentCharacterBolt(long persistenceFrequencyHz,
			int tweetLength, int emissionFreq, String description,
			String throughputInfo) {

		super(persistenceFrequencyHz, tweetLength, emissionFreq, description,
				throughputInfo);

		// _persistencePeriodNanos = (long) (1.0 / persistenceFrequencyHz *
		// NANOS_IN_SECOND);
		// _tweetLength = tweetLength;
		// _emissionFrequency = emissionFreq;
		// _description = description;
		// _networkBandwidth = throughputInfo;
	}

	public MostFrequentCharacterBolt(long persistenceFrequencyHz,
			int tweetLength, int emissionFreq, String description) {
		this(persistenceFrequencyHz, tweetLength, emissionFreq, description,
				"NO INFO");
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	protected Values process(Tuple tuple) {
		String text = tuple.getValueByField("text").toString();
		for (int i = 0; i < text.length(); i++) {
			_charsCounter.count(text.charAt(i));
		}

		return new Values(_charsCounter.getMaxResultCharacter(),
				_charsCounter.getMaxResultCount());

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("character", "count"));
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
