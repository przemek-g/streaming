package fr.inria.streaming.simulation.data;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FakePersister implements ICountPersister {

	private static final long serialVersionUID = -7977133571254967658L;

	private static Logger logger = Logger.getLogger(FakePersister.class
			.getName());

	private static DateTimeFormatter _dateTimeFormatter = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

	@Override
	public void persistCounterWithCurrentTimestamp(
			InvocationsCounter invocationsCounter, String description,
			String elementType, String bandwidth, int tweetLenght,
			long emissionFrequency) {

		String msg = new StringBuilder("persisting counter: ")
				.append(invocationsCounter.getCount()).append(", at: ")
				.append(_dateTimeFormatter.print(DateTime.now()))
				.append("; description: ").append(description)
				.append("; network throughput: ").append(bandwidth)
				.append(", type: ").append(elementType).toString();

		logger.info(msg);

	}

	@Override
	public String toString() {
		return "instance-of-" + this.getClass().getName();
	}

}
