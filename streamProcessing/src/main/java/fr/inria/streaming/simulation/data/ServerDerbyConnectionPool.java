package fr.inria.streaming.simulation.data;

public class ServerDerbyConnectionPool extends DerbyDatabaseConnectionPool {
	
	private static final String _BASE_URL = "jdbc:derby://localhost:8000/";
	private static final String _DEFAULT_DB_NAME = "simulation-DB";
	private static final String _CLIENT_DRIVER_CLASS_NAME = "org.apache.derby.jdbc.ClientDriver";

	private static ServerDerbyConnectionPool _instance;
	
	public static synchronized ServerDerbyConnectionPool getInstance() {
		if (_instance == null) {
			_instance = new ServerDerbyConnectionPool();
		}
		return _instance;
	}
	
	private ServerDerbyConnectionPool() {
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
