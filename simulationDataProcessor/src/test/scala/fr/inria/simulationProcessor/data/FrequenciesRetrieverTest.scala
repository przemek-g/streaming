package fr.inria.simulationProcessor.data

import org.junit._
import Assert._
import java.sql.Connection
import java.sql.Statement
import java.sql.SQLException
import org.apache.log4j.Logger
import org.scalatest.Assertions._

@Test
class FrequenciesRetrieverTest {

  val _logger: Logger = Logger.getLogger(this.getClass())

  val _url = "jdbc:derby:test1-db;create=true";
  val _createTableStr = "create table counter_values(timestamp TIMESTAMP , count BIGINT, element_type VARCHAR(6), bandwidth VARCHAR(15), tweet_length INTEGER, emission_frequency_Hz INTEGER, description VARCHAR(50))"
  val _deleteTableStr = "delete from counter_values"

  def _createTables() = {
    try {
      var conn: Connection = ConnectionProvider(_url)
      var stmt: Statement = conn.createStatement()
      stmt.executeUpdate(_createTableStr)
      conn.commit()
    } catch {
      case sqlEx: SQLException => {
        _logger.warn("Exception during creation of table counter_values for " + _url + ": " + sqlEx.toString)
      }
    }
  }

  def _prepareDatabase(inserts1: List[String]) = {

    _createTables()
    var conn: Connection = ConnectionProvider(_url)
    var stmt: Statement = conn.createStatement()

    inserts1.foreach(s => stmt.executeUpdate(s))
    conn.commit()
  }

  @After
  def _clearDatabase() = {
    try {
      var conn: Connection = ConnectionProvider(_url)
      var stmt: Statement = conn.createStatement()
      stmt.executeUpdate(_deleteTableStr)
      conn.commit()
    } catch {
      case sqlEx: SQLException => _logger.warn("Exception during deletion of table counter_values for " + _url + ": " + sqlEx.toString)
    }
  }

  /* ---------------------------------------------------------------------------------------------------------------------------- */
  /* Testing methods */
  /* ---------------------------------------------------------------------------------------------------------------------------- */
  
  @Test
  def testWithSeveralValues() : Unit = {
    var inserts: List[String] = List[String](
      "insert into counter_values values('2012-04-05 12:00:05', 5, 'SPOUT', '2Mbit/s', 100, 200, 'some_description')",
      "insert into counter_values values('2012-04-05 12:00:06', 6, 'SPOUT', '2Mbit/s', 100, 400, 'some_description')",
      "insert into counter_values values('2012-04-05 12:00:07', 7, 'SPOUT', '2Mbit/s', 100, 200, 'some_description')",
      "insert into counter_values values('2012-04-05 12:00:08', 8, 'SPOUT', '2Mbit/s', 100, 800, 'some_description')",
      "insert into counter_values values('2012-04-05 12:00:08', 8, 'SPOUT', '4Mbit/s', 100, 1000, 'some_description')", // note: a different bandwidth
      "insert into counter_values values('2012-04-05 12:00:08', 8, 'SPOUT', '2Mbit/s', 50, 1200, 'some_description')") // note: a different tweetLength

    _prepareDatabase(inserts)

    var frequencies = new FrequenciesRetriever("2Mbit/s", 100).getDistinctFrequencyValues(_url)
    assertNotNull(frequencies)
    assertResult(3){ frequencies.size }
   
    frequencies = frequencies.sortWith((x,y) => x<y)
    assertEquals(200, frequencies(0))
    assertEquals(400, frequencies(1))
    assertEquals(800, frequencies(2))
 
    frequencies.foreach(x => _logger debug x)
  }

}