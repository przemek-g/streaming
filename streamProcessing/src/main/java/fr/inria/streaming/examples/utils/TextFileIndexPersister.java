package fr.inria.streaming.examples.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import fr.inria.streaming.examples.utils.index.TrieInvertedCountingIndex;

public class TextFileIndexPersister implements IndexPersister, Serializable {

	private static final long serialVersionUID = -5030180595273577314L;

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
//			logger.info("Trying to get writer for file "+file.getName());
			writer = TextFileWritersFactory.getWriterFor(file);
		}
		
		Set<String> indexKeys = index.getKeys();
		
		boolean hasEntries = indexKeys.size()>0;
		
		try {
			
			synchronized (writer) {
				if (hasEntries) {
					writer.write("[\n");
				}
				
				for (String key : indexKeys) {
					Map<String,Integer> indexRecords = index.getEntriesForKey(key);
					
					if (indexRecords != null) {
						writer.write("{'key':"+key+",\n");
						writer.write("'records':[\n");
						
						for (Entry<String,Integer> record : indexRecords.entrySet()) {
							writer.write("{'docId':"+record.getKey()+", 'count':"+record.getValue().toString()+"},\n");
						}
						
					}
					
					if (hasEntries) {
						writer.write("] },\n");
					}
					
				}
				
				if (hasEntries) {
					writer.write("]\n");
					writer.flush();
				}
			}
			
			
		} catch (IOException e) {
			logger.error("When writing to file: "+file.getAbsolutePath(),e);
		}
	}

	
}
