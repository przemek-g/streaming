package fr.inria.streaming.simulation.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class JdbcCounterPersister implements ICountPersister {

	// ------- static fields and methods ---
	// -------------------------------------
	private static final long serialVersionUID = -3881458904541129080L;
	private static Logger logger = Logger.getLogger(JdbcCounterPersister.class);

	private static final String _CREATE_TABLE = "create table counter_values(timestamp TIMESTAMP , count BIGINT, element_type VARCHAR(6), bandwidth VARCHAR(15), description VARCHAR(50))";
	private static DateTimeFormatter _dateTimeFormatter = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

	// by default, use the connection provider for the embedded mode
	public static IDatabaseConnectionProvider _connectionProvider = EmbeddedDerbyConnectionPool.getInstance();
	private static Statement _stmt;

	private static Map<String, JdbcCounterPersister> _persistersStore = new HashMap<String, JdbcCounterPersister>();

	public static void setConnectionProvider(
			IDatabaseConnectionProvider provider) {
		if (provider != null) {
			_connectionProvider = provider;
		}
	}

	/**
	 * Retrieve a singleton JdbcCounterPersister using the given
	 * IDatabaseConnectionProvider.
	 * 
	 * @param desc
	 * @return null if the JdbcCountPersister cannot be instantiated because of
	 *         database connection errors
	 */
	public static synchronized JdbcCounterPersister getInstance(String dbName) {
		if (!_persistersStore.containsKey(dbName)) {
			try {
				_persistersStore.put(dbName, new JdbcCounterPersister(dbName,
						_connectionProvider));
			} catch (Exception e) {
				logger.error(e.toString());
				return null;
			}
		}
		return _persistersStore.get(dbName);
	}

	// ----------- instance fields and methods ---
	// -------------------------------------------
	private transient Connection _conn;

	private JdbcCounterPersister(String dbName,
			IDatabaseConnectionProvider provider) throws Exception {
		try {
			_conn = provider.getCustomConnection(dbName);
			if (_conn == null) {
				throw new Exception(
						"Could not instantiate JdbcCounterPersister for db "
								+ dbName
								+ " because the connection is not available!");
			}
			_stmt = _conn.createStatement();
			_stmt.executeUpdate(_CREATE_TABLE);
			_conn.commit();
			logger.info("Created table with command: " + _CREATE_TABLE);
		} catch (SQLException e) {
			logger.warn("SQLExcpetion while instantiating JdbcCounterPersister: "
					+ e.toString());
		}
	}

	@Override
	public void persistCounterWithCurrentTimestamp(
			InvocationsCounter invocationsCounter, String description,
			String recordType, String bandwidth) {
		try {

			String msg = new StringBuilder("persisting counter: ")
					.append(invocationsCounter.getCount()).append(", at: ")
					.append(_dateTimeFormatter.print(DateTime.now()))
					.toString();

			logger.info(msg);

			String insertStr = new StringBuilder(
					"insert into counter_values values(").append("'")
					.append(_dateTimeFormatter.print(DateTime.now()))
					.append("'").append(", ")
					// timestamp
					.append(invocationsCounter.getCount()).append(", ")
					// count
					.append("'").append(recordType).append("'").append(", ")
					// element_type (SPOUT or BOLT)
					.append("'").append(bandwidth).append("'").append(", ")
					// network throughput info
					.append("'").append(description).append("'").append(")") 
					.toString();
			_stmt.executeUpdate(insertStr);

		} catch (SQLException e) {
			logger.error("SQLException during insert: " + e.toString());
		}
	}

	public Connection getConnection() {
		return _conn;
	}

	@Override
	public String toString() {
		return "instance-of="+this.getClass().getName();
	}
	
}
