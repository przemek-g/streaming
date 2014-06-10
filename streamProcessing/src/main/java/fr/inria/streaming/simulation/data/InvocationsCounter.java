package fr.inria.streaming.simulation.data;

import java.util.HashMap;
import java.util.Map;

public class InvocationsCounter {
	
	// --- static ---
	private static Map<String, InvocationsCounter> _instancesStore = new HashMap<String, InvocationsCounter>();
	
	public static synchronized InvocationsCounter getInstance(String name) {
		if (!_instancesStore.containsKey(name)) {
			_instancesStore.put(name, new InvocationsCounter());
		}
		return _instancesStore.get(name);
	}
	
	// --- instance ---
	
	private volatile long _count;
	
	private InvocationsCounter() {
		_count = 0;
	}
	
	public void increment() {
		_count++;
	}
	
	public long getCount() {
		return _count;
	}
}
