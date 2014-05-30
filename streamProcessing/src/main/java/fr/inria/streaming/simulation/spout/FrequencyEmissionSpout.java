package fr.inria.streaming.simulation.spout;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import fr.inria.streaming.simulation.util.TextContentSource;
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

	private TextContentSource _textContentSource;
	private SpoutOutputCollector _outputCollector;
	private long _frequencyNs;
	private ScheduledExecutorService _scheduledExecutor;
	
	public FrequencyEmissionSpout(long frequencyNs, TextContentSource source) {
		_frequencyNs = frequencyNs;
		_textContentSource = source;
		_scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_outputCollector = collector;
		// schedule emission of a tuple at fixed rate with 0 delay
		_scheduledExecutor.scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				nextTuple();
			}
		}, 0, _frequencyNs, TimeUnit.NANOSECONDS);
	}

	@Override
	public void nextTuple() {
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
		_scheduledExecutor.shutdownNow();
	}
	

}
