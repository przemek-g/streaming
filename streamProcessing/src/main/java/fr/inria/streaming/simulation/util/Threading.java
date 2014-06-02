package fr.inria.streaming.simulation.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Threading {
	
	private static ScheduledExecutorService service;
	
	public static synchronized ScheduledExecutorService getScheduledExecutorService() {
		if (service == null) {
			service = Executors.newSingleThreadScheduledExecutor();
		}
		return service;
	}

}
