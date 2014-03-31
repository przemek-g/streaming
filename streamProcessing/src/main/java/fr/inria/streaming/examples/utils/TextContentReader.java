package fr.inria.streaming.examples.utils;

import java.io.Serializable;

public interface TextContentReader extends Serializable{

	static class NoContentAvailableException extends Exception {
		
		public NoContentAvailableException(String msg) {
			super(msg);
		}
	}
	
	String getContent() throws NoContentAvailableException;
}
