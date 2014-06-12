package fr.inria.streaming.simulation.data;

import java.io.Serializable;

public interface ICountPersister extends Serializable {

	void persistCounterWithCurrentTimestamp(
			InvocationsCounter invocationsCounter, String description,
			String recordType, String bandwidth, int tweetLength,
			long emissionFrequency);
}
