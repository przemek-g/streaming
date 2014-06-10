package fr.inria.streaming.simulation.spout;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import fr.inria.streaming.simulation.Simulation;
import fr.inria.streaming.simulation.data.ICountPersister;
import fr.inria.streaming.simulation.data.InvocationsCounter;
import fr.inria.streaming.simulation.data.PersistenceManager;
import fr.inria.streaming.simulation.util.ITextContentSource;
import fr.inria.streaming.simulation.util.ThreadsManager;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A Spout that emits a new portion of data with the given frequency.
 * 
 * @author Przemek
 * 
 */
public class FrequencyEmissionSpout extends BaseRichSpout {

	// --- static ---
	private static final long serialVersionUID = 1L;

	private static final long NANOS_IN_SECOND = 1000000000;

	private static Logger logger = Logger
			.getLogger(FrequencyEmissionSpout.class);

	private static long _intendedEmissionsCounter = 0;
	private static InvocationsCounter _invocationsCounter = InvocationsCounter
			.getInstance(FrequencyEmissionSpout.class.getName());
	private static long _persistenceCounter = 0;

	public static long getEmissionsCounter() {
		return _invocationsCounter.getCount();
	}

	public static long getPersistenceCount() {
		return _persistenceCounter;
	}

	// --- instance ---
	private ITextContentSource _textContentSource;
	private ICountPersister _persister;
	private SpoutOutputCollector _outputCollector;

	private long _emissionPeriodNanoseconds;
	private long _persistencePeriodNanoSeconds;
	private String _description;
	private final String _elementType = "SPOUT";
	private String _networkThroughput;

	public FrequencyEmissionSpout(long frequencyHertz, String description,
			String throughputInfo, ITextContentSource source) {
		this(frequencyHertz, 1, description, throughputInfo, source);
	}

	public FrequencyEmissionSpout(long emissionFrequencyHertz,
			long persistenceFrequencyHertz, String description,
			String throughputInfo, ITextContentSource source) {
		_emissionPeriodNanoseconds = (long) (1.0 / emissionFrequencyHertz * NANOS_IN_SECOND);
		_persistencePeriodNanoSeconds = (long) (1.0 / persistenceFrequencyHertz * NANOS_IN_SECOND);
		_textContentSource = source;
		_description = description;
		_networkThroughput = throughputInfo;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		String openingMsg = new StringBuilder("Opening of ")
				.append(this.getClass().getName())
				.append(" with emission period of ")
				.append(_emissionPeriodNanoseconds).append(" [ns]")
				.append(" and persistence period of ")
				.append(_persistencePeriodNanoSeconds).append(" [ns]")
				.toString();

		logger.info(openingMsg);
		_outputCollector = collector;

		this._persister = PersistenceManager.getPersisterInstance(conf);
		logger.info("FrequencyEmissionSpout set its persister to the following one:"+this._persister.toString());
		
		// schedule emission of a tuple at fixed rate with 0 delay
		ThreadsManager.getScheduledExecutorService().scheduleAtFixedRate(
				new Runnable() {
					@Override
					public void run() {
						_intendedEmissionsCounter++;

						logger.info(new StringBuilder(
								"Spout INTENDED emission nr ")
								.append(_intendedEmissionsCounter)
								.append(" ...").toString());

						emitTuple();
					}
				}, 0, _emissionPeriodNanoseconds, TimeUnit.NANOSECONDS);

		// schedule persistence of the counter at fixed rate
		ThreadsManager.getScheduledExecutorService().scheduleAtFixedRate(
				new Runnable() {
					@Override
					public void run() {
						_persistenceCounter++;

						logger.info(new StringBuilder(
								"Spout INTENDED persistence nr ")
								.append(_persistenceCounter).append("...")
								.toString());

						_persister.persistCounterWithCurrentTimestamp(
								_invocationsCounter, _description,
								_elementType, _networkThroughput);
					}
				}, 0, _persistencePeriodNanoSeconds, TimeUnit.NANOSECONDS);
	}

	/*
	 * (non-Javadoc) Not doing anything here, since I don't want spouts to
	 * emitted when the system calls me!
	 */
	@Override
	public void nextTuple() {
		try {
			Thread.sleep(100); // the documentation says it's good to sleep here
								// so as not to waste too much cpu
		} catch (InterruptedException e) {
			logger.warn("Thread interrupted while sleeping for 100 ms in nextTuple: "
					+ e.toString());
		}
	}

	/**
	 * Emits a text tuple via SpoutOutputCollector. This method is used by the
	 * auxiliary thread that invokes tuple emissions with a given frequency.
	 */
	private void emitTuple() {
		char[] text = this._textContentSource.getTextContent();

		if (text != null) {

			String txtStr = new String(text);
			_invocationsCounter.increment();
			logger.info(new StringBuilder("Spout emission nr ")
					.append(_invocationsCounter.getCount()).append(" : ")
					.append(txtStr).toString());

			this._outputCollector.emit(new Values(txtStr));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("text"));
	}

	@Override
	public void close() {
		ThreadsManager.getScheduledExecutorService().shutdownNow();
	}

}
