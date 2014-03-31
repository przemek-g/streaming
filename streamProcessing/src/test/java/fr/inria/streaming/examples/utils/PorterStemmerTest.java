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
	public void test() {
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

		str = "year 2000 July 29th";
		stemmer.add(str.toCharArray(), str.length());
		stemmer.stem();
		result = stemmer.toString();
		logger.debug(result);
		assertEquals("year 2000 July 29th",result);
		
	}
}
