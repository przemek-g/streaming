package fr.inria.streaming.examples.bolt;

import java.util.Map;

import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class IndexingBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6659754042191631119L;

	// the structure for storing our records - a simple memory-based implementation here
	private TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
	
	@Override
	public void execute(Tuple tuple) {
		String docId = tuple.getString(0);
		String word = tuple.getString(1);
		
		// normally we increment count in the index by one...
		int count = 1;
		
		// ... if not stated other explicitly in the tuple (it's more flexible that way)
		try {
			count = tuple.getInteger(2);
		} catch (Exception e) { }
		
		this.index.increaseEntryForKey(word, docId, count);
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
	
	public TrieInvertedCountingIndex getIndex() {
		return this.index;
	}
	
	public void setIndex(TrieInvertedCountingIndex index) {
		this.index = index;
	}

}
