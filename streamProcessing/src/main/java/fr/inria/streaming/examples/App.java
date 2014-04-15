package fr.inria.streaming.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fr.inria.streaming.examples.bolt.IndexingBolt;
import fr.inria.streaming.examples.bolt.SentenceSplittingBolt;
import fr.inria.streaming.examples.bolt.WordStemmingBolt;
import fr.inria.streaming.examples.spout.TextContentSpout;
import fr.inria.streaming.examples.spout.TextContentSpout.WrongFileNameException;
import fr.inria.streaming.examples.utils.TextFileIndexPersister;
import fr.inria.streaming.examples.utils.TextFileReader;

/**
 * Hello world!
 *
 */
public class App 
{
	private static Logger logger = Logger.getLogger(App.class);

	static String getDesiredFileName(String[] args) {
		if (args == null) {
			return null;
		}
		// command line options handling
		Options options = new Options();
		options.addOption(new Option("f",true,"The file to be processed"));
		try {
			CommandLine cmd = new BasicParser().parse(options, args);
			String fileOption = cmd.getOptionValue("f");
			if (fileOption != null) {
				return fileOption;
			}
		} catch (ParseException e) {
			logger.error("A problem occurred during parsing");
		}
		
		return null;
	}
	
    public static void main( String[] args ) throws InterruptedException, WrongFileNameException
    {
    	TopologyBuilder builder = new TopologyBuilder();

    	String fileName = "PanTadeusz.txt";

    	// ------------------------------------------------------
    	String commandLineFileName = getDesiredFileName(args);
    	if (commandLineFileName != null) {
    		fileName = commandLineFileName;
    	}
    	// ------------------------------------------------------
    	
    	logger.info("The file name is "+fileName);
    	
        builder.setSpout("text-spout", new TextContentSpout(fileName),1); 
        builder.setBolt("splitter-bolt", new SentenceSplittingBolt(), 4).shuffleGrouping("text-spout");
        builder.setBolt("stemmer-bolt", new WordStemmingBolt(),4).shuffleGrouping("splitter-bolt");
        builder.setBolt("index-bolt", new IndexingBolt(new TextFileIndexPersister("AppIndex.txt")),4).fieldsGrouping("stemmer-bolt", new Fields("docId"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sample-streaming-topology", conf, builder.createTopology());

        Thread.sleep(4000);

        cluster.shutdown();
    }
}
