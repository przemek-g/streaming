package fr.inria.streaming.simulation.data;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.fest.assertions.api.Assertions.*;
import static org.junit.Assert.*;

/**
 * Integration test of counter persistence.
 * @author Przemek
 *
 */
public class JdbcCounterPersisterTest {
	
	private static Logger logger = Logger.getRootLogger();
	
	private static JdbcCounterPersister persister;
	private static IDatabaseConnectionProvider connectionProvider;
	
	private static final String selectCountRows = "select count(*) from counter_values";
	private static final String selectAll = "select * from counter_values";
	
	private static final String _testDB = "test-db";
	
	@BeforeClass
	public static void setUpBeforeClass() throws IOException {
		connectionProvider = EmbeddedDerbyConnectionPool.getInstance();
		JdbcCounterPersister.setConnectionProvider(connectionProvider);
		persister = JdbcCounterPersister.getInstance(_testDB); 
	}
	
	@Before
	public void setUpBefore() throws SQLException {
		Statement stmt = connectionProvider.getCustomConnection(_testDB).createStatement();
		stmt.execute("delete from counter_values");
	}
	
	@Test
	public void testSimpleInsert() throws SQLException {
		InvocationsCounter counter = InvocationsCounter.getInstance(this.getClass().getName());
		long counterValue = 0;
		
		Statement stmt = connectionProvider.getCustomConnection(_testDB).createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		ResultSet rs;
		
		List<Long> counterValues = new ArrayList<Long>();
		for (int i=0; i< 10; i++) {
			rs = stmt.executeQuery(selectCountRows);
			rs.next();
			assertSame(rs.getInt(1), counterValues.size());
//			assertThat(rs.getInt(1)).isEqualTo(counterValues.size());
			
			rs = stmt.executeQuery(selectAll);
			if (rs.last()) { // set the cursor to the last row
				assertThat(rs.getLong("count")).isEqualTo(counterValue);
			}
			
			counter.increment();
			counterValue = counter.getCount();
			counterValues.add(counterValue);
			persister.persistCounterWithCurrentTimestamp(counter, "testing persistence", "test", "test network");
		}
	}
	
	@AfterClass
	public static void tearDownAfterClass() {
		try {
			connectionProvider.getCustomConnection(_testDB).close();
		} catch (SQLException e) {
			logger.error("SQLException during closing connection: "+e.toString());
		}
	}

}
