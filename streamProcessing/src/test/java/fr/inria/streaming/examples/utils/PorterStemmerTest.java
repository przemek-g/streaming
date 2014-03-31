package fr.inria.streaming.examples.utils;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class PorterStemmerTest {

	private static Logger logger;
	private static PorterStemmer stemmer;
	
	@BeforeClass
	public static void init() {
		stemmer = new PorterStemmer();
		logger = Logger.getLogger(PorterStemmerTest.class);
	}
	
	@Test
	public void testRegularWordsInput() {
		String str = "programming";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		String result = stemmer.toString();
		logger.debug(result);
		assertEquals("program",result);
		
		str = "computing";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("comput",result);

		str = "machines";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("machin",result);
		
		str = "performance";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("perform",result);

		str = "researchers";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("research",result);
	}
	
	@Test
	public void testRootFormsInput() {
		String str = "program";
		stemmer.add(str.toCharArray(),str.length());
		stemmer.stem();
		String result = stemmer.toString();
		logger.debug(result);
		assertEquals("program",result);
		
		str = "comput";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("comput",result);
		
		str = "machin";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("machin",result);
		
		str = "perform";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("perform",result);
		
		str = "research";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("research",result);
	}
	
	@Test
	public void testStrangeWordsInput() {
		String str = "year 2000 July 29th";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		String result = stemmer.toString();
		logger.debug(result);
		assertEquals(str, result);
		
		str = "abcdefghijklmnop";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "a b c d";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "notEvenASingleWord";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = " What kind\tof\nword\tdo we \nhave ";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
	}
	
	@Test
	public void testBlankAndWhiteSpaceInput() {
		String str = "";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		String result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = " ";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "      ";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "\t";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "\n";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = "\n";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
		str = " \n \t \r";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals(str,result);
		
	}
}
