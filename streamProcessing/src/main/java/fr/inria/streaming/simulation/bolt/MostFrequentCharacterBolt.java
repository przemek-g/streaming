package fr.inria.streaming.simulation.bolt;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.Simulation;
import fr.inria.streaming.simulation.data.FakePersister;
import fr.inria.streaming.simulation.data.ICountPersister;
import fr.inria.streaming.simulation.data.InvocationsCounter;
import fr.inria.streaming.simulation.data.PersistenceManager;
import fr.inria.streaming.simulation.util.CharactersCounter;
import fr.inria.streaming.simulation.util.ThreadsManager;
import fr.inria.streaming.simulation.util.TupleUtils;

public class MostFrequentCharacterBolt extends BaseRichBolt {

	// --- static ---
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger
			.getLogger(MostFrequentCharacterBolt.class);
	private static long _persistenceCounter = 0;
	private static final long NANOS_IN_SECOND = 1000000000;
	private static InvocationsCounter _invocationsCounter = InvocationsCounter
			.getInstance(MostFrequentCharacterBolt.class.getName());

	public static long getExecutionsCount() {
		return _invocationsCounter.getCount();
	}

	public static long getPersistenceCount() {
		return _persistenceCounter;
	}

	// --- instance ---
	private OutputCollector _outputCollector;
	private CharactersCounter _charsCounter = new CharactersCounter();

	private ICountPersister _persister;
	private long _persistencePeriodNanos;
	private String _description;
	private String _elementType = "BOLT";
	private String _networkBandwidth;
	private int _tweetLength;
	private int _emissionFrequency;

	public MostFrequentCharacterBolt(long persistenceFrequencyHz,
			int tweetLength, int emissionFreq, String description,
			String throughputInfo) {
		_persistencePeriodNanos = (long) (1.0 / persistenceFrequencyHz * NANOS_IN_SECOND);
		_tweetLength = tweetLength;
		_emissionFrequency = emissionFreq;
		_description = description;
		_networkBandwidth = throughputInfo;
	}

	public MostFrequentCharacterBolt(long persistenceFrequencyHz,
			int tweetLength, int emissionFreq, String description) {
		this(persistenceFrequencyHz, tweetLength, emissionFreq, description,
				"NO INFO");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;

		logger.info("Preparing Bolt: " + this.getClass().getName());

		this._persister = PersistenceManager.getPersisterInstance(stormConf);
		logger.info("MostFrequentCharacterBolt set its persister to the following one: "
				+ _persister.toString());

		// start a thread for the persister here!!!
		ThreadsManager.getScheduledExecutorService().scheduleAtFixedRate(
				new Runnable() {
					@Override
					public void run() {
						_persistenceCounter++;

						logger.info(new StringBuilder(
								"Bolt INTENDED persistence nr ")
								.append(_persistenceCounter).append("...")
								.toString());

						_persister.persistCounterWithCurrentTimestamp(
								_invocationsCounter, _description,
								_elementType, _networkBandwidth, _tweetLength,
								_emissionFrequency);
					}
				}, 0, _persistencePeriodNanos, TimeUnit.NANOSECONDS);
	}

	@Override
	public void execute(Tuple input) {
		if (!TupleUtils.isTickTuple(input)) {
			process(input);
		} else {
			logger.info("BOLT: SimpleConsumerBolt got a tick tuple!");
		}
	}

	private void process(Tuple tuple) {
		String text = tuple.getValueByField("text").toString();
		for (int i = 0; i < text.length(); i++) {
			_charsCounter.count(text.charAt(i));
		}

		_invocationsCounter.increment();

		Values valuesToSend = new Values(_charsCounter.getMaxResultCharacter(),
				_charsCounter.getMaxResultCount());

		logger.info(new StringBuilder("Bolt emission nr ")
				.append(_invocationsCounter.getCount()).append(" : ")
				.append(valuesToSend.toString()).toString());

		// after processing every text, send the current maximum result further
		// down the stream
		
		_outputCollector.emit(valuesToSend);
		// if using Storm's reliability mechanisms, acknowledge the incoming tuple
		_outputCollector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("character", "count")); // not really
															// important what we
															// declare here,
															// just emit
															// something
	}

}
