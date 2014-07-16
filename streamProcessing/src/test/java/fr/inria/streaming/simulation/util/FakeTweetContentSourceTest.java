package fr.inria.streaming.simulation.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class FakeTweetContentSourceTest {

	@Test
	public void testTweetLength() {
		FakeTweetContentSource.setTweetLength(80);
		FakeTweetContentSource source = new FakeTweetContentSource();
		char[] tweet = source.getTextContent();
		assertEquals(80, tweet.length);
		
		FakeTweetContentSource.setTweetLength(13);
		tweet = source.getTextContent();
		assertEquals(13, tweet.length);
		
		FakeTweetContentSource.setTweetLength(-1);
		tweet = source.getTextContent();
		assertEquals(13, tweet.length);
		
		FakeTweetContentSource.setTweetLength(0);
		tweet = source.getTextContent();
		assertEquals(13, tweet.length);
		
		FakeTweetContentSource.setTweetLength(1);
		tweet = source.getTextContent();
		assertEquals(1, tweet.length);
		
		FakeTweetContentSource.setTweetLength(20);
		tweet = source.getTextContent();
		assertEquals(20, tweet.length);
		
		FakeTweetContentSource.setTweetLength(400);
		tweet = source.getTextContent();
		assertEquals(400, tweet.length);
		
		FakeTweetContentSource.setTweetLength(5000);
		tweet = source.getTextContent();
		assertEquals(5000, tweet.length);
	}
}
