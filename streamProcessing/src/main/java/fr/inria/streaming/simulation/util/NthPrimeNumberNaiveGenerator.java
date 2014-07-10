package fr.inria.streaming.simulation.util;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

public class NthPrimeNumberNaiveGenerator {

	private static Logger _logger = Logger
			.getLogger(NthPrimeNumberNaiveGenerator.class);

	private boolean isPrime(long n) {
		if (n < 2)
			return false;

		for (long i = 2; i < n; i++) {
			if (n % i == 0) {
				return false;
			}
		}

		return true;
	}

	public long generateNthPrime(int n) {
		if (n < 1)
			return -1;

		int primesCounter = 0;
		long i = 2;

		while (primesCounter < n) {
			i++;
			if (isPrime(i)) {
				primesCounter++;
			}
		}

		return i;
	}

	public static void main(String... args) {
		if (args.length < 1) {
			_logger.info("Too few arguments! Tell me which prime number I should compute");
		}

		int n;
		try {
			n = Integer.valueOf(args[0]);
			DateTime beginning = DateTime.now();
			long nthPrime = new NthPrimeNumberNaiveGenerator()
					.generateNthPrime(n);
			DateTime ending = DateTime.now();
			Period period = new Period(beginning, ending);
			PeriodFormatter formatter = new PeriodFormatterBuilder()
					.printZeroAlways().appendMinutes().appendSeparator(":")
					.appendSecondsWithMillis().toFormatter();
			_logger.info(n+"th prime number is "+nthPrime+". It took "+formatter.print(period)+" to calculate.");
			
		} catch (Exception e) {
			_logger.error("Sorry, but you should give an integer");
		}

	}
}
