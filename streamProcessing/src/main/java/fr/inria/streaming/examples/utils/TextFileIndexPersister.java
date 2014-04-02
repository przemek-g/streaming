package fr.inria.streaming.examples.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;

public class TextFileIndexPersister implements IndexPersister {

	private static Logger logger = Logger.getLogger(TextFileIndexPersister.class);
	
	private File file;
	private BufferedWriter writer;
	
	public TextFileIndexPersister(String fileName) {
		this.file = new File(fileName);
	}
	
	@Override
	public void persist(TrieInvertedCountingIndex index) {
		
		if (index == null) {
			return;
		}
		
		try {
			file.createNewFile(); // atomically checks for existence and creates a file if needed
		} catch (IOException e) {
			logger.error("While creating the file: "+file.getAbsolutePath(), e);
			return;
		} 
		
		if (writer == null) {
			try {
				writer = new BufferedWriter(new FileWriter(file));
			} catch (IOException e) {
				logger.error("While creating a writer for file: "+file.getAbsolutePath(), e);
				return;
			}
		}
		
		try {
			writer.write("[\n");
			
			for (String key : index.getKeys()) {
				Map<String,Integer> indexRecords = index.getEntriesForKey(key);
				writer.write("{'key':"+key+",\n");
				writer.write("'records':[\n");
				
				for (Entry<String,Integer> record : indexRecords.entrySet()) {
					writer.write("{'docId':"+record.getKey()+", 'count':"+record.getValue().toString()+"},\n");
				}
				writer.write("]\n},\n");
			}
			writer.write("]\n");
			writer.flush();
			
		} catch (IOException e) {
			logger.error("When writing to file: "+file.getAbsolutePath(),e);
		}
	}

	
}
