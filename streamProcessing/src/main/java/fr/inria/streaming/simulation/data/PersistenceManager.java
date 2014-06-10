package fr.inria.streaming.simulation.data;

import java.util.Map;

import org.apache.log4j.Logger;

public class PersistenceManager {

	private static Logger _logger = Logger.getLogger(PersistenceManager.class.getName());
	private static final String _DB_NAME = "sim-db";
	
	public static final String DB_CONNECTION_MODE = "db-connection-mode";
	public static final String SERVER_DB = "server";
	public static final String EMBEDDED_DB = "embedded";
	
	public static ICountPersister getPersisterInstance(Map conf) {
		
		if (conf == null) {
			_logger.warn("No conf given. Returning a FakePersister instance");
			return new FakePersister();
		}
		
		String  connectionMode = (String) conf.get(DB_CONNECTION_MODE);
		if (connectionMode != null) {
			if (SERVER_DB.equals(connectionMode)) {
				JdbcCounterPersister.setConnectionProvider(ServerDerbyConnectionPool.getInstance());
				_logger.info("Returning JdbcCounterPersister with ServerDerbyConnectionPool");
				return JdbcCounterPersister.getInstance(_DB_NAME);
			}
			if (EMBEDDED_DB.equals(connectionMode)) {
				JdbcCounterPersister.setConnectionProvider(EmbeddedDerbyConnectionPool.getInstance());
				_logger.info("Returning JdbcCounterPersister with EmbeddedDerbyConnectionPool");
				return JdbcCounterPersister.getInstance(_DB_NAME);
			}
		}
		
		_logger.info("Returning a FakePersister instance");
		return new FakePersister();
	}
}
