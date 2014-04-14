package fr.inria.streaming.examples.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class TextFileWritersFactory {

	private static Logger logger = Logger.getLogger(TextFileWritersFactory.class);
	
	private static Map<String, BufferedWriter> writers = new HashMap<String,BufferedWriter>();
	
	public static synchronized BufferedWriter getWriterFor(File file) {
		if (file == null) {
			return null;
		}
		
		if (!writers.containsKey(file.getName())) {
			try {
				writers.put(file.getName(), new BufferedWriter(new FileWriter(file.getName())));
			} catch (IOException e) {
				logger.error("Could not create FileWriter for file: "+file.getName());
			}
		}
		
		BufferedWriter writer = writers.get(file.getName());
		
		return writer;
	}
}
