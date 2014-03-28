package fr.inria.streaming.examples.utils;


import static org.junit.Assert.assertEquals;

import org.junit.Test;

import fr.inria.streaming.examples.utils.TextContentReader.NoContentAvailableException;

public class TextFileReaderTest {
	
	@Test(expected=NoContentAvailableException.class)
	public void nonExistentFile() throws NoContentAvailableException {
		String nonExistentFile = "non-existent-file.txt"; // this file should not be present on the class path ;-)
		TextFileReader textFileReader = new TextFileReader(nonExistentFile);
		textFileReader.getContent();
	}

	@Test(expected=NoContentAvailableException.class)
	public void testWithEmptyFile() throws NoContentAvailableException {
		String file = "empty-file.txt";
		TextFileReader reader = new TextFileReader(file);
		
		String line = reader.getContent();
	}
	
	@Test(expected=NoContentAvailableException.class)
	public void testWithfileContainingFiveLines() throws NoContentAvailableException {
		String file = "five-lines-file.txt";
		TextFileReader reader = new TextFileReader(file);
		
		String line; 
		
		for (int i=1; i<=5; i++) {
			line = reader.getContent();
			String expectedLine = "";
			
			switch (i) {
				case 1: expectedLine = "first"; break;
				case 2: expectedLine = "second"; break;
				case 3: expectedLine = "third"; break;
				case 4: expectedLine = "fourth"; break;
				case 5: expectedLine = "fifth"; break;
			}
			
			expectedLine += " line";
			assertEquals(expectedLine, line);
		}
		
		reader.getContent();
		
	}
	
}
