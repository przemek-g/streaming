package fr.inria.streaming.examples.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class WordsRecognizerTest {

	private WordsRecognizer wr = new WordsRecognizer();
	
	@Test
	public void properWordsTest() {
		String word = "something";
		boolean result = wr.isStringAWord(word);
		assertTrue(result);
		
		word = "abc";
		result = wr.isStringAWord(word);
		assertTrue(result);
	}
	
	@Test
	public void improperWordsTest() {
		String word = "some longer phrase";
		boolean result = wr.isStringAWord(word);
		assertFalse(result);
		
		word = "   ";
		result = wr.isStringAWord(word);
		assertFalse(result);
		
		word = " \n \t";
		result = wr.isStringAWord(word);
		assertFalse(result);
		
		word = null;
		result = wr.isStringAWord(word);
		assertFalse(result);
		
		word = "";
		result = wr.isStringAWord(word);
		assertFalse(result);
	}
}
