package fr.inria.streaming.examples.bolt;

import java.util.Map;

import fr.inria.streaming.examples.utils.SentenceSplitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentenceSplittingBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5595810020198932934L;

	private OutputCollector collector;
	private SentenceSplitter splitter = new SentenceSplitter();
	
	
	@Override
	public void execute(Tuple tuple) {
		String docId = tuple.getString(0);
		String line = tuple.getString(1);

		String[] words = splitter.splitSentence(line);
		
		if (words != null) {
			for (String word : words) {
				collector.emit(tuple, new Values(docId,word));
			}
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("docId","word"));
	}

}
