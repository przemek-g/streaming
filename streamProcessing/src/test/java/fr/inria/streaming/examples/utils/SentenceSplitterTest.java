package fr.inria.streaming.examples.utils;

import org.junit.Test;
import static org.junit.Assert.*;

public class SentenceSplitterTest {

	private SentenceSplitter splitter = new SentenceSplitter();
	
	@Test
	public void testProperSentences() {
		String sentence = "This is some fancy sentence, isn't it?";
		String[] result = this.splitter.splitSentence(sentence);
		String[] expectedResult = new String[]{"This","is","some","fancy","sentence,","isn't","it?"};
		assertArrayEquals(expectedResult, result);
		
		sentence = "Hello";
		result = this.splitter.splitSentence(sentence);
		expectedResult = new String[]{"Hello"};
		assertArrayEquals(expectedResult, result);
		
		sentence = " And this one is tricky\n \t";
		result = this.splitter.splitSentence(sentence);
		expectedResult = new String[]{"And","this","one","is","tricky"};
		assertArrayEquals(expectedResult, result);
	}
	
	@Test
	public void testImproperSentences() {
		String sentence = " ";
		String[] result = this.splitter.splitSentence(sentence);
		String[] expectedResult = null;
		assertArrayEquals(expectedResult, result);
		
		sentence = "";
		result = this.splitter.splitSentence(sentence);
		expectedResult = null;
		assertArrayEquals(expectedResult, result);
		
		sentence = "\n\t  \t";
		result = this.splitter.splitSentence(sentence);
		expectedResult = null;
		assertArrayEquals(expectedResult, result);
	}
}
