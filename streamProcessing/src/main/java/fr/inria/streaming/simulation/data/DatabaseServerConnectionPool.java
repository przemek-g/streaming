package fr.inria.streaming.simulation.data;

public class DatabaseServerConnectionPool extends DerbyDatabaseConnectionPool {
	
	private static final String _BASE_URL = "jdbc:derby://localhost:1527/";
	private static final String _DEFAULT_DB_NAME = "simulation-DB";
	private static final String _CLIENT_DRIVER_CLASS_NAME = "org.apache.derby.jdbc.ClientDriver";

	private static DatabaseServerConnectionPool instance;
	
	/**
	 * Returns the singleton instance of this class.
	 */
	public static synchronized DatabaseServerConnectionPool getInstance() {
		if (instance == null) {
			instance = new DatabaseServerConnectionPool();
		}
		return instance;
	}
	
	private DatabaseServerConnectionPool() {
		super();
	}

	// --- define the abstract methods ---
	
	@Override
	protected String getBaseUrl() {
		return _BASE_URL;
	}
	
	@Override
	protected String getDefaultDbName() {
		return _DEFAULT_DB_NAME;
	}
	
	@Override
	protected String getDriverName() {
		return _CLIENT_DRIVER_CLASS_NAME;
	}

}
