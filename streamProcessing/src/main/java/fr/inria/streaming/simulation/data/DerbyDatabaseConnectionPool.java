package fr.inria.streaming.simulation.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public abstract class DerbyDatabaseConnectionPool implements IDatabaseConnectionProvider {

	// ----- static stuff -----
	private static Logger _logger = Logger.getLogger(DerbyDatabaseConnectionPool.class.getName());
	private static String _createDB = ";create=true;";
	
	// ----- the abstract methods ------
	protected abstract String getBaseUrl();
	protected abstract String getDefaultDbName();
	protected abstract String getDriverName();
	
	private Map<String,Connection> _connections = new HashMap<String,Connection>();
	
	/**
	 * @param dbName name of the database, e.g. "my-simulation-DB"
	 * @return a String identifier: BASE_URL + dbName + ";create=true;"
	 */
	private String createDatabaseUrl(String dbName) {
		return new StringBuilder(getBaseUrl()).append(dbName).append(_createDB).toString();
	}
	
	@Override
	public synchronized Connection getCustomConnection(String dbName) {
		
		if (dbName == null) {
			return null;
		}
		
		dbName = createDatabaseUrl(dbName);
		
		if (!_connections.containsKey(dbName)) {
			try {
				Class.forName(getDriverName()).newInstance();
				_connections.put(dbName, DriverManager.getConnection(dbName));
			} catch (InstantiationException e) {
				_logger.error("Error instantiating SQL Connection: "+e.toString());
			} catch (IllegalAccessException e) {
				_logger.error("Illegal access to database: "+e.toString());
			} catch (ClassNotFoundException e) {
				_logger.error("The class could not be loaded: "+e.toString());
				e.printStackTrace();
			} catch (SQLException e) {
				_logger.error("SQLExcpetion: "+e.toString());
			}
		}
		
		return _connections.get(dbName);
	}
	

}
