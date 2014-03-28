package fr.inria.streaming.examples.utils;

public interface TextContentReader {

	static class NoContentAvailableException extends Exception {
		
		public NoContentAvailableException(String msg) {
			super(msg);
		}
	}
	
	String getContent() throws NoContentAvailableException;
}
