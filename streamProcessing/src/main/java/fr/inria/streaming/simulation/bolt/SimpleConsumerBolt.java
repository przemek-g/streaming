package fr.inria.streaming.simulation.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fr.inria.streaming.simulation.util.CharactersCounter;
import fr.inria.streaming.simulation.util.TupleUtils;

public class SimpleConsumerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector _outputCollector;
	private CharactersCounter _charsCounter = new CharactersCounter();
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_outputCollector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		if (!TupleUtils.isTickTuple(input)) {
			process(input);
		}
	}

	private void process(Tuple tuple) {
		String text = tuple.getValueByField("text").toString();
		for (int i=0; i<text.length(); i++) {
			_charsCounter.count(text.charAt(i));
		}
		// after processing every text, send the current maximum result further down the stream
		_outputCollector.emit(new Values(_charsCounter.getMaxResultCharacter(), _charsCounter.getMaxResultCount()));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("character","count")); // not really important what we declare here, just emit something
	}

}
