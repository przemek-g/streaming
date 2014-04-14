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
import fr.inria.streaming.examples.utils.TextFileReader;

public class TextContentSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = -2314788623424320028L;

	private static Logger logger = Logger.getLogger(TextContentSpout.class);
	
	private String title;
	private TextContentReader textContentReader;
	private SpoutOutputCollector collector;
	
	public TextContentSpout(String title) throws WrongFileNameException {
		if (title == null || title.length()==0) {
			throw new WrongFileNameException("incorrect title: "+title);
		}
		this.title = title;
		this.textContentReader = new TextFileReader(title);
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		try {
			String content = this.textContentReader.getContent();
			this.collector.emit(new Values(title,content));
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
		declarer.declare(new Fields("docId","line"));
	}
	
	public static class WrongFileNameException extends Exception {

		private static final long serialVersionUID = -4676893691243848609L;
		
		public WrongFileNameException(String s) {
			super(s);
		}
		
	}

}
