package fr.inria.streaming.simulation.spout;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import fr.inria.streaming.simulation.util.ITextContentSource;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A Spout that emits a new portion of data with the given frequency.
 * @author Przemek
 *
 */
public class FrequencyEmissionSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private static final long NANOS_IN_SECOND = 1000000000;
	
	private ITextContentSource _textContentSource;
	private SpoutOutputCollector _outputCollector;
	private long _periodNanoseconds;
	private ScheduledExecutorService _scheduledExecutor;
	
	private boolean _closed = true;
	
	public FrequencyEmissionSpout(long frequencyHertz, ITextContentSource source) {
		_periodNanoseconds = (long)(1.0/frequencyHertz*NANOS_IN_SECOND);
		_textContentSource = source;
		_scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		this.markOpen();
		_outputCollector = collector;
		// schedule emission of a tuple at fixed rate with 0 delay
		_scheduledExecutor.scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				nextTuple();
			}
		}, 0, _periodNanoseconds, TimeUnit.NANOSECONDS);
	}

	@Override
	public void nextTuple() {
		if (this._closed) {
			return;
		}
		char[] text = this._textContentSource.getTextContent();
		if (text != null) {
			this._outputCollector.emit(new Values(text));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("text"));
	}
	
	@Override
	public void close() {
		this.markClosed();
		_scheduledExecutor.shutdownNow();
	}
	
	private void markClosed() {
		this._closed = true;
	}
	
	private void markOpen() {
		this._closed = false;
	}

}
