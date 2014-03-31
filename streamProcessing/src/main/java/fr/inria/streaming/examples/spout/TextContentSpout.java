package fr.inria.streaming.examples.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import fr.inria.streaming.examples.utils.TextContentReader;
import fr.inria.streaming.examples.utils.TextContentReader.NoContentAvailableException;

public class TextContentSpout extends BaseRichSpout {
	
	private static Logger logger = Logger.getLogger(TextContentSpout.class);
	
	private TextContentReader textContentReader;
	private SpoutOutputCollector collector;
	
	public TextContentSpout(TextContentReader reader) {
		this.textContentReader = reader;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		try {
			String content = this.textContentReader.getContent();
			this.collector.emit(new Values(content));
		} catch (NoContentAvailableException e) {
			logger.error("Content not available for streaming");
		}
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
