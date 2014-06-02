package fr.inria.streaming.simulation;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import fr.inria.streaming.simulation.bolt.SimpleConsumerBolt;
import fr.inria.streaming.simulation.spout.FrequencyEmissionSpout;
import fr.inria.streaming.simulation.util.FakeTweetContentSource;
import fr.inria.streaming.simulation.util.Threading;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class Simulation {

	private static Logger logger = Logger.getLogger(Simulation.class); 
	
	private static Options prepareCmdLineOptions() {
		Options options = new Options();
		options.addOption("t", "topologyName", true, "The name of the simulation topology");
		
		options.addOption("s","spoutName", true, "The name of the spout to be emitted");
		options.addOption("b", "boltName", true, "The name of the bolt that shall receive tuples");
		
		options.addOption("ss", "spoutSupervisorName", true, "The name of the supervisor host that shall execute the spout");
		options.addOption("bs", "boltSupervisorName", true, "The name of the supervisor host that shall execute the bolt");
		
		options.addOption("freq","frequencyHz",true,"Frequency of emission in the spout");
		options.addOption("sleep","sleepTime",true,"Time in seconds to spend sleeping in local mode, before killing the topology");
		options.addOption(new Option("d", "distributed", false, "If set, deploys the topology in distributed mode"));
		return options;
	}
	
	public static void main(String[] args) {
		
		try {
			CommandLine cmd = new BasicParser().parse(prepareCmdLineOptions(), args);
			String topology = cmd.getOptionValue("topologyName");
			String spout = cmd.getOptionValue("spoutName");
			String spoutSupervisor = cmd.getOptionValue("spoutSupervisorName");
			String bolt = cmd.getOptionValue("boltName");
			String boltSupervisor = cmd.getOptionValue("boltSupervisorName");
			String frequencyHertz = cmd.getOptionValue("frequencyHz", "1");
			boolean isDistributedMode = cmd.hasOption("distributed");
			
			logger.info("Topology name: "+topology);
			logger.info("Spout name: "+spout);
			logger.info("Spout supervisor name: "+spoutSupervisor);
			logger.info("Bolt name: "+bolt);
			logger.info("Bolt supervisor: "+boltSupervisor);
			logger.info("Is in distributed mode: "+isDistributedMode);
			
			Config conf = new Config();
			conf.setDebug(true);
			// the following config setting doesn't work on a per-topology basis, it's a matter of the whole cluster's configuration
//			conf.put(Config.STORM_SCHEDULER, "fr.inria.streaming.simulation.scheduler.SimulationTopologyScheduler");
			
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout(spout, new FrequencyEmissionSpout(Long.valueOf(frequencyHertz), new FakeTweetContentSource()),1);
			builder.setBolt(bolt, new SimpleConsumerBolt(),1).shuffleGrouping(spout);
			
			if (isDistributedMode) {
				conf.setNumWorkers(2);
				StormSubmitter.submitTopology(topology, conf, builder.createTopology());
			}
			else {
				String sleepTimeStr = cmd.getOptionValue("sleepTime", "15");
				long numOfSecondsToSleep;
				numOfSecondsToSleep = Long.valueOf(sleepTimeStr)*1000;
				
//				Config clusterConf = new Config();
//				clusterConf.put(Config.STORM_SCHEDULER,"fr.inria.streaming.simulation.scheduler.SimulationTopologyScheduler");
				
				LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(topology, conf, builder.createTopology());
				
				Thread.sleep(numOfSecondsToSleep);
				localCluster.shutdown();
				
				Threading.getScheduledExecutorService().shutdownNow();
			}
			
		} catch (ParseException e) {
			logger.error("Error during parsing command line options !!!:" + e.toString());
		} catch (AlreadyAliveException e) {
			logger.error("The topology is already alive !!!:" + e.toString());
		} catch (InvalidTopologyException e) {
			logger.error("The topology name is invalid !!!:" + e.toString());
		} catch (InterruptedException e) {
			logger.error("The thread has been interrupted: " + e.toString());
		}
	}
}
