package fr.inria.streaming.simulation.data;

public class EmbeddedDerbyConnectionPool extends DerbyDatabaseConnectionPool {

	private static final String _BASE_URL = "jdbc:derby:";
	private static final String _DEFAULT_DB_NAME = "simulation-embedded-DB";
	private static final String _EMBEDDED_DRIVER_CLASS_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	
	private static EmbeddedDerbyConnectionPool _instance;
	
	public static synchronized EmbeddedDerbyConnectionPool getInstance() {
		if (_instance == null) {
			_instance = new EmbeddedDerbyConnectionPool();
		}
		return _instance;
	}
	
	private EmbeddedDerbyConnectionPool() {
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
		return _EMBEDDED_DRIVER_CLASS_NAME;
	}

}
