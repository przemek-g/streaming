package fr.inria.streaming.examples.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class TextFileReader implements TextContentReader{

	private String fileName;
	private Scanner fileScanner;
	private String noContentInFileMsg;
	private String fileNotFoundMsg;
	
	public TextFileReader(String fileName) {
		this.fileName = fileName;
		this.noContentInFileMsg = "No more content in file "+this.fileName;
		this.fileNotFoundMsg = String.format("File %s was not found", this.fileName);
	}
	
	@Override
	public String getContent() throws NoContentAvailableException {
		if (fileScanner == null) {
			try {
				fileScanner = new Scanner(getClass().getClassLoader().getResourceAsStream(fileName));
			} catch (NullPointerException e) {
				throw new NoContentAvailableException(this.fileNotFoundMsg);
			}
		}
		
		if (fileScanner.hasNextLine()) {
			return fileScanner.nextLine();
		}
		
		throw new NoContentAvailableException(noContentInFileMsg);
	}

}
