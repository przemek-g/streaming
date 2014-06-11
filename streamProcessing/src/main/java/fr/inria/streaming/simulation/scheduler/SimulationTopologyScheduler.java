package fr.inria.streaming.simulation.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class SimulationTopologyScheduler implements IScheduler {

	/*
	 * Parameters to be set by the invoking app.
	 * Their setters only set the new value when it's not null (to preserve the default ones in case none are given).
	 */	
	private static String topologyName = "simulation-topology";
	private static String spoutName = "simulation-spout";
	private static String boltName = "simulation-bolt";
	private static String boltSupervisorName = "bolt-supervisor";
	private static String spoutSupervisorName = "spout-supervisor";

	private static Logger logger = Logger.getLogger(SimulationTopologyScheduler.class.getName());
	
	
	@Override
	public void prepare(Map conf) { }

	/* (non-Javadoc)
	 * For the topology given by topologyName: 
	 *  - schedules all the executors for the bolt given by boltName to the supervisor given by boltSupervisorName
	 *  - schedules all the executors for the spout given by spoutName to the supervisor given by spoutSupervisorName
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {

		logger.info(this.getClass().getName()+" - scheduling for the topology !");
		TopologyDetails seekedTopology = topologies.getByName(topologyName);
		
		if (seekedTopology != null) {
			
			boolean topologyNeedsScheduling = cluster.needsScheduling(seekedTopology);
			
			if (!topologyNeedsScheduling) {
				String msgTopologyDoesntNeedSchedule = new StringBuilder("The topology: ").append(topologyName).append(" does not need scheduling").toString();
				logger.info(msgTopologyDoesntNeedSchedule);
			}
			else {
				String msgTopologyNeedsSchedule = new StringBuilder("The topology: ").append(topologyName).append(" needs scheduling").toString();
				logger.info(msgTopologyNeedsSchedule);
				
				Map<String,List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(seekedTopology);
				SchedulerAssignment currentAssignment = cluster.getAssignmentById(seekedTopology.getId());
				
				if (currentAssignment != null) {
					logger.info("Current assignment is: "+currentAssignment.getExecutorToSlot());
				}
				else {
					logger.info("Current assignments is empty: {}");
				}
				
				// --- SCHEDULE THE BOLT to its supervisor -----------
				if (!componentToExecutors.containsKey(boltName)) {
					
					String msgBoltDoesntNeedSchedule = new StringBuilder("The bolt: ").append(boltName).append(" does not need scheduling").toString();
					logger.info(msgBoltDoesntNeedSchedule);
				}
				else {
					String msgBoltNeedsSchedule = new StringBuilder("The bolt: ").append(boltName).append(" needs scheduling").toString();
					logger.info(msgBoltNeedsSchedule);
					
					List<ExecutorDetails> executors = componentToExecutors.get(boltName);
					
					// let's find our supervisor
					Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
					SupervisorDetails seekedSupervisor = null; 
					
					for (SupervisorDetails supervisor : supervisors) {
						Map meta = (Map) supervisor.getSchedulerMeta();
						
						if (meta.get("name").equals(boltSupervisorName)) {
							seekedSupervisor = supervisor;
							break;
						}
					}
					
					if (seekedSupervisor != null) {
						logger.info("Found the supervisor for bolt: " + boltSupervisorName);
						
						List<WorkerSlot> availableSlots = cluster.getAvailableSlots(seekedSupervisor);
						
						// if there are no available slots on this supervisor, we need to free some of them
						if (availableSlots.isEmpty() && !executors.isEmpty()) {
							// here we free all the worker ports on our supervisor
							for (Integer port : cluster.getUsedPorts(seekedSupervisor)) {
								cluster.freeSlot(new WorkerSlot(seekedSupervisor.getId(),port));
							}
						}
						
						// again, get the available slots in our destination supervisor
						availableSlots = cluster.getAvailableSlots(seekedSupervisor);
						WorkerSlot chosenSlot = availableSlots.get(0);
						
						// assign all the executors of our spout to one process slot
						cluster.assign(chosenSlot, seekedTopology.getId(), executors);
						String msgAssignedExecutors = new StringBuilder(
								"The bolt executors: ").append(executors)
								.append(" have been assigned to slot: ")
								.append(chosenSlot.getNodeId()).append(" : ")
								.append(chosenSlot.getPort()).toString();
						logger.info(msgAssignedExecutors);
					}
					else {
						String msgNoSupervisor = new StringBuilder("The supervisor named ").append(boltSupervisorName).append(" NOT FOUND").toString();
						logger.info(msgNoSupervisor);
					}
				} // end if componentToExecutors.containsKey( boltName )
				
				// ----- SCHEDULE THE SPOUT to its supervisor ----------
				if (!componentToExecutors.containsKey(spoutName)) {
					
					String msgSpoutDoesntNeedSchedule = new StringBuilder("The spout: ").append(spoutName).append(" does not need scheduling").toString();
					logger.info(msgSpoutDoesntNeedSchedule);
				}
				else {
					String msgSpoutNeedsSchedule = new StringBuilder("The spout: ").append(spoutName).append(" needs scheduling").toString();
					logger.info(msgSpoutNeedsSchedule);
					
					List<ExecutorDetails> executors = componentToExecutors.get(spoutName);
					
					// let's find our supervisor
					Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
					SupervisorDetails seekedSupervisor = null; 
					
					for (SupervisorDetails supervisor : supervisors) {
						Map meta = (Map) supervisor.getSchedulerMeta();
						
						if (meta.get("name").equals(spoutSupervisorName)) {
							seekedSupervisor = supervisor;
							break;
						}
					}
					
					if (seekedSupervisor != null) {
						logger.info("Found the supervisor " + boltSupervisorName);
						
						List<WorkerSlot> availableSlots = cluster.getAvailableSlots(seekedSupervisor);
						
						// if there are no available slots on this supervisor, we need to free some of them
						if (availableSlots.isEmpty() && !executors.isEmpty()) {
							// here we free all the worker ports on our supervisor
							for (Integer port : cluster.getUsedPorts(seekedSupervisor)) {
								cluster.freeSlot(new WorkerSlot(seekedSupervisor.getId(),port));
							}
						}
						
						// again, get the available slots in our destination supervisor
						availableSlots = cluster.getAvailableSlots(seekedSupervisor);
						WorkerSlot chosenSlot = availableSlots.get(0);
						
						// assign all the executors of our spout to one process slot
						cluster.assign(chosenSlot, seekedTopology.getId(), executors);
						String msgAssignedExecutors = new StringBuilder(
								"The spout executors: ").append(executors)
								.append(" have been assigned to slot: ")
								.append(chosenSlot.getNodeId()).append(" : ")
								.append(chosenSlot.getPort()).toString();
						logger.info(msgAssignedExecutors);
					}
					else {
						String msgNoSupervisor = new StringBuilder("The supervisor named ").append(spoutSupervisorName).append(" NOT FOUND").toString();
						logger.info(msgNoSupervisor);
					}
				} // end if componentToExecutors.containsKey( spoutName )
				
			} // end if topologyNeedsScheduling
		}
		else {
			String msg = new StringBuilder("The topology: ").append(topologyName).append(" NOT FOUND").toString();
			logger.info(msg);
		} // end if seekedTopology!=null
		
		// the rest of the scheduling job can be done by Storm's EvenScheduler
		new EvenScheduler().schedule(topologies, cluster);
	}
	
	public static String getBoltSupervisorName() {
		return boltSupervisorName;
	}
	
	public static void setBoltSupervisorName(String boltSupervisorName) {
		if (boltSupervisorName != null) {
			SimulationTopologyScheduler.boltSupervisorName = boltSupervisorName;
		}
	}
	
	public static String getSpoutSupervisorName() {
		return spoutSupervisorName;
	}
	
	public static void setSpoutSupervisorName(String spoutSupervisorName) {
		if (spoutSupervisorName != null) {
			SimulationTopologyScheduler.spoutSupervisorName = spoutSupervisorName;
		}
	}
	
	public static String getSpoutName() {
		return spoutName;
	}
	
	public static void setSpoutName(String spoutName) {
		if (spoutName != null) {
			SimulationTopologyScheduler.spoutName = spoutName;
		}
	}
	
	public static String getBoltName() {
		return boltName;
	}
	
	public static void setBoltName(String boltName) {
		if (boltName != null) {
			SimulationTopologyScheduler.boltName = boltName;
		}
	}
	
	public static String getTopologyName() {
		return topologyName;
	}
	
	public static void setTopologyName(String topologyName) {
		if (topologyName != null) {
			SimulationTopologyScheduler.topologyName = topologyName;
		}
	}
}
