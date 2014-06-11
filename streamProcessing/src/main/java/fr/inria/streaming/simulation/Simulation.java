package fr.inria.streaming.simulation;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import fr.inria.streaming.simulation.bolt.MostFrequentCharacterBolt;
import fr.inria.streaming.simulation.data.PersistenceManager;
import fr.inria.streaming.simulation.scheduler.SimulationTopologyScheduler;
import fr.inria.streaming.simulation.spout.FrequencyEmissionSpout;
import fr.inria.streaming.simulation.util.FakeTweetContentSource;
import fr.inria.streaming.simulation.util.ThreadsManager;

public class Simulation {

	private static Logger _logger = Logger.getLogger(Simulation.class);

	private static Options prepareCmdLineOptions() {
		Options options = new Options();
		options.addOption("t", "topologyName", true,
				"The name of the simulation topology");

		options.addOption("s", "spoutName", true,
				"The name of the spout to be emitted");
		options.addOption("b", "boltName", true,
				"The name of the bolt that shall receive tuples");

		options.addOption("ss", "spoutSupervisorName", true,
				"The name of the supervisor host that shall execute the spout");
		options.addOption("bs", "boltSupervisorName", true,
				"The name of the supervisor host that shall execute the bolt");

		options.addOption("ef", "emissionFrequency", true,
				"Frequency of emission in the spout");
		options.addOption("pf", "persistenceFrequency", true,
				"Frequency of persistence of the spout's data");

		options.addOption("sleep", "sleepTime", true,
				"Time in seconds to spend sleeping in local mode, before killing the topology");

		options.addOption(new Option("d", "distributed", false,
				"If set, deploys the topology in distributed mode"));

		options.addOption(new Option(
				"pt",
				"persistenceType",
				true,
				"Describes the kind of database to use: embedded, server or fake (the default one)"));

		options.addOption("desc", "description", true,
				"Additional description of the simulation.");

		options.addOption(
				"nt",
				"throughput",
				true,
				"Information on the network throughput in the format: 'value unit',"
						+ "where unit should have the following form: Bit/s, KBit/s, MBit/s, Byte/s, KByte/s, MByte/s, etc.");

		options.addOption("tl", "tweetLength", true,
				"Length of a single tweet that gets emitted into the channel");

		return options;
	}

	public static void main(String[] args) {

		try {
			CommandLine cmd = new BasicParser().parse(prepareCmdLineOptions(),
					args);
			String topology = cmd.getOptionValue("topologyName");
			String spout = cmd.getOptionValue("spoutName");
			String spoutSupervisorName = cmd
					.getOptionValue("spoutSupervisorName");
			String bolt = cmd.getOptionValue("boltName");
			String boltSupervisorName = cmd
					.getOptionValue("boltSupervisorName");
			String emissionFrequencyHertz = cmd.getOptionValue(
					"emissionFrequency", "1");
			String persistenceFrequencyHertz = cmd.getOptionValue(
					"persistenceFrequency", "1");
			boolean isDistributedMode = cmd.hasOption("distributed");
			String persistenceType = cmd.getOptionValue("persistenceType",
					"fake");
			String description = cmd.getOptionValue("description");
			String tweetLength = cmd.getOptionValue("tweetLength");
			String throughput = cmd.getOptionValue("throughput");

			if (topology == null) {
				topology = SimulationTopologyScheduler.getTopologyName();
			}
			if (spout == null) {
				spout = SimulationTopologyScheduler.getSpoutName();
			}
			if (spoutSupervisorName == null) {
				spoutSupervisorName = SimulationTopologyScheduler
						.getSpoutSupervisorName();
			}
			if (bolt == null) {
				bolt = SimulationTopologyScheduler.getBoltName();
			}
			if (boltSupervisorName == null) {
				boltSupervisorName = SimulationTopologyScheduler
						.getBoltSupervisorName();
			}
			if (persistenceType == null) {
				persistenceType = "fake";
			}
			if (description == null) {
				description = "example simulation data";
			}
			if (tweetLength != null) {
				FakeTweetContentSource.setTweetLength(Integer
						.valueOf(tweetLength));
			}
			if (throughput == null) {
				throughput = "NO INFO";
			}

			_logger.info("Topology name: " + topology);
			_logger.info("Spout name: " + spout);
			_logger.info("Spout supervisor name: " + spoutSupervisorName);
			_logger.info("Bolt name: " + bolt);
			_logger.info("Bolt supervisor: " + boltSupervisorName);
			_logger.info("Is in distributed mode: " + isDistributedMode);
			_logger.info("Frequency of emission [Hz]: "
					+ emissionFrequencyHertz);
			_logger.info("Frequency of persistence [Hz]: "
					+ persistenceFrequencyHertz);
			_logger.info("Persistence type: " + persistenceType);
			_logger.info("Description: " + description);
			_logger.info("Tweet length is: "
					+ FakeTweetContentSource.getTweetLength());
			_logger.info("Network link throughput is:" + throughput);

			Config conf = new Config();
			conf.setDebug(true);
			// no need to check for illegal values, etc. - will result in a
			// FakePersister in such cases
			conf.put(PersistenceManager.DB_CONNECTION_MODE, persistenceType);
			// the following config setting doesn't work on a per-topology
			// basis, it's a matter of the whole cluster's configuration
			// conf.put(Config.STORM_SCHEDULER,
			// "fr.inria.streaming.simulation.scheduler.SimulationTopologyScheduler");

			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout(
					spout,
					new FrequencyEmissionSpout(Long
							.valueOf(emissionFrequencyHertz), Long
							.valueOf(persistenceFrequencyHertz), description
							+ " - spout", throughput,
							new FakeTweetContentSource()), 1).setNumTasks(1);
			builder.setBolt(
					bolt,
					new MostFrequentCharacterBolt(Long
							.valueOf(persistenceFrequencyHertz), description
							+ " - bolt", throughput), 1).setNumTasks(1)
					.shuffleGrouping(spout);

			if (isDistributedMode) {
				conf.setNumWorkers(2);
				StormSubmitter.submitTopology(topology, conf,
						builder.createTopology());
			} else {
				String sleepTimeStr = cmd.getOptionValue("sleepTime", "15");
				_logger.info("sleepTime [s]: " + sleepTimeStr);
				long numOfSecondsToSleep;
				numOfSecondsToSleep = Long.valueOf(sleepTimeStr) * 1000;

				LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(topology, conf,
						builder.createTopology());

				_logger.info("Now the main thread is going to sleep for "
						+ numOfSecondsToSleep + " s...");
				Thread.sleep(numOfSecondsToSleep);

				_logger.info("Main thread woke up, now shutting down the other threads...");
				ThreadsManager.getScheduledExecutorService().shutdownNow();

				_logger.info("Now shutting down the cluster ...");
				localCluster.shutdown();

				String summaryMsg = new StringBuilder(
						"Shutdown complete. The number of messages emitted by the spout is: ")
						.append(FrequencyEmissionSpout.getEmissionsCounter())
						.append(", the number of execution invocations on the bolt is: ")
						.append(MostFrequentCharacterBolt.getExecutionsCount())
						.append(". The number of spout persistence invocations is: ")
						.append(FrequencyEmissionSpout.getPersistenceCount())
						.append(", and the number of bolt persistence invocations is: ")
						.append(MostFrequentCharacterBolt.getPersistenceCount())
						.toString();

				_logger.info(summaryMsg);
			}

		} catch (ParseException e) {
			_logger.error("Error during parsing command line options !!!:"
					+ e.toString());
		} catch (AlreadyAliveException e) {
			_logger.error("The topology is already alive !!!:" + e.toString());
		} catch (InvalidTopologyException e) {
			_logger.error("The topology name is invalid !!!:" + e.toString());
		} catch (InterruptedException e) {
			_logger.error("The thread has been interrupted: " + e.toString());
		}
	}
}
