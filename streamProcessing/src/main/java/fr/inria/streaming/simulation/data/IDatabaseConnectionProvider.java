package fr.inria.streaming.simulation.data;

import java.sql.Connection;

public interface IDatabaseConnectionProvider {

	Connection getCustomConnection(String dbName);
}
