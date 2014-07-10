package fr.inria.streaming.simulation.bolt;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import fr.inria.streaming.simulation.data.ICountPersister;
import fr.inria.streaming.simulation.data.InvocationsCounter;
import fr.inria.streaming.simulation.data.PersistenceManager;
import fr.inria.streaming.simulation.util.ThreadsManager;
import fr.inria.streaming.simulation.util.TupleUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class CountingBolt extends BaseRichBolt {

	/* static stuff */
	private static final long serialVersionUID = 7905237493919539408L;
	protected static final long NANOS_IN_SECOND = 1000000000;

	/* instance stuff */
	protected OutputCollector _outputCollector;

	protected ICountPersister _persister;
	protected long _persistencePeriodNanos;
	protected String _description;
	protected String _elementType = "BOLT";
	protected String _networkBandwidth;
	protected int _tweetLength;
	protected int _emissionFrequency;

	protected CountingBolt(long persistenceFrequencyHz, int tweetLength,
			int emissionFreq, String description, String throughputInfo) {
		_persistencePeriodNanos = (long) (1.0 / persistenceFrequencyHz * NANOS_IN_SECOND);
		_tweetLength = tweetLength;
		_emissionFrequency = emissionFreq;
		_description = description;
		_networkBandwidth = throughputInfo;
	}

	public abstract long getPersistenceCount();

	protected abstract Logger getLogger();

	protected abstract Values process(Tuple tuple);

	protected abstract void incrementPersistenceCount();

	protected abstract InvocationsCounter getInvocationsCounter();

	@Override
	public void execute(Tuple input) {
		if (!TupleUtils.isTickTuple(input)) {

			// count this invocation
			getInvocationsCounter().increment();

			// perform the bolt-specific processing of the input tuple
			Values valuesToSend = process(input);

			getLogger().info(
					new StringBuilder("Bolt emission nr ")
							.append(getInvocationsCounter().getCount())
							.append(" : ").append(valuesToSend.toString())
							.toString());

			// send the tuple further down into the stream
			_outputCollector.emit(valuesToSend);

			// if using Storm's reliability mechanisms, acknowledge the incoming
			// tuple
			_outputCollector.ack(input);
		} else {
			getLogger().info("BOLT: " + getClass() + " got a tick tuple!");
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;

		getLogger().info("Preparing Bolt: " + this.getClass().getName());

		this._persister = PersistenceManager.getPersisterInstance(stormConf);
		getLogger().info(
				"MostFrequentCharacterBolt set its persister to the following one: "
						+ _persister.toString());

		// start a thread for the persister here!!!
		ThreadsManager.getScheduledExecutorService().scheduleAtFixedRate(
				new Runnable() {
					@Override
					public void run() {
						incrementPersistenceCount();

						getLogger().info(
								new StringBuilder(
										"Bolt INTENDED persistence nr ")
										.append(getPersistenceCount())
										.append("...").toString());

						_persister.persistCounterWithCurrentTimestamp(
								getInvocationsCounter(), _description,
								_elementType, _networkBandwidth, _tweetLength,
								_emissionFrequency);
					}
				}, 0, _persistencePeriodNanos, TimeUnit.NANOSECONDS);
	}
}
