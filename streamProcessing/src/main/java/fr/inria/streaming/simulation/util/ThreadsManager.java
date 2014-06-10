package fr.inria.streaming.simulation.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ThreadsManager {
	
	private static ScheduledExecutorService service;
	
	public static synchronized ScheduledExecutorService getScheduledExecutorService() {
		if (service == null) {
//			service = Executors.newSingleThreadScheduledExecutor();
			service = Executors.newScheduledThreadPool(2);
		}
		return service;
	}

}
