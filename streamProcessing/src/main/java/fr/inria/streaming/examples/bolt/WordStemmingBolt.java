package fr.inria.streaming.examples.bolt;

import java.util.Map;

import fr.inria.streaming.examples.utils.PorterStemmer;
import fr.inria.streaming.examples.utils.WordsRecognizer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordStemmingBolt extends BaseRichBolt {

	private static final long serialVersionUID = -7275390652127788057L;
	private OutputCollector collector;
	private PorterStemmer stemmer = new PorterStemmer();

	private WordsRecognizer wr = new WordsRecognizer();
	
	@Override
	public void execute(Tuple tuple) {
		String docId = tuple.getString(0);
		String originalWord = tuple.getString(1);
		
		// stem this word
		this.stemmer.add(originalWord.toCharArray(), originalWord.length());
		this.stemmer.stem();
		String stemWord = this.stemmer.toString();
		
		// emit the stemmed form further
		if (wr.isStringAWord(stemWord)) {
			collector.emit(tuple, new Values(docId,stemWord));
			collector.ack(tuple); // ack for Storm
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("docId","stemmed-word"));
	}

}
