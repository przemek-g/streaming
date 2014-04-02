package fr.inria.streaming.examples.bolt;

import java.util.Map;

import fr.inria.streaming.examples.utils.IndexPersister;
import fr.inria.streaming.examples.utils.TupleUtils;
import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class IndexingBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6659754042191631119L;

	// the structure for storing our records - a simple memory-based implementation here
	private TrieInvertedCountingIndex index = new TrieInvertedCountingIndex();
	
	private OutputCollector collector;
	private IndexPersister indexPersister;
	

	@Override
	public void execute(Tuple tuple) {
		
		// periodically persist the index structure to some data source
		if (TupleUtils.isTickTuple(tuple)) {
			if (indexPersister != null) {
				indexPersister.persist(getIndex());
			}
		}
		else {
			String docId = tuple.getString(0);
			String word = tuple.getString(1);
			
			// normally we increment count in the index by one...
			int count = 1;
			
			// ... if not stated other explicitly in the tuple (it's more flexible that way)
			try {
				count = tuple.getInteger(2);
			} catch (Exception e) { }
			
			this.index.increaseEntryForKey(word, docId, count);
			
			collector.ack(tuple); // ack for Storm
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
	
	@Override
	public Map<String,Object> getComponentConfiguration() {
		Config conf = new Config();
		int tickFrequencyInSeconds = 3;
		
		// a component-specific conf parameter for receiving a special tuple from '__system' component and '__tick' stream
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		
		return conf;
	}
	
	public TrieInvertedCountingIndex getIndex() {
		return this.index;
	}
	
	public void setIndex(TrieInvertedCountingIndex index) {
		this.index = index;
	}

	public IndexPersister getIndexPersister() {
		return indexPersister;
	}
	
	public void setIndexPersister(IndexPersister indexPersister) {
		this.indexPersister = indexPersister;
	}
}
