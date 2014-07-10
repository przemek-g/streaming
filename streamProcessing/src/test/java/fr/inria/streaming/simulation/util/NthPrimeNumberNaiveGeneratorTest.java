package fr.inria.streaming.simulation.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class NthPrimeNumberNaiveGeneratorTest {

	private NthPrimeNumberNaiveGenerator gen = new NthPrimeNumberNaiveGenerator();
	
	@Test
	public void testIllegalArgs() {
		long result = gen.generateNthPrime(-3);
		assertEquals(-1, result);
		
		result = gen.generateNthPrime(-1);
		assertEquals(-1, result);
		
		result = gen.generateNthPrime(0);
		assertEquals(-1, result);
	}
	
	@Test
	public void testLegalArgs() {
		long result = gen.generateNthPrime(1);
		assertEquals(3, result);
		
		result = gen.generateNthPrime(2);
		assertEquals(5, result);
		
		result = gen.generateNthPrime(3);
		assertEquals(7, result);
		
		result = gen.generateNthPrime(4);
		assertEquals(11, result);
		
		result = gen.generateNthPrime(5);
		assertEquals(13, result);
	}
	
}
