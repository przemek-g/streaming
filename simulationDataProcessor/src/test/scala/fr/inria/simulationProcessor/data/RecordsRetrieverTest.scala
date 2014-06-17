package fr.inria.simulationProcessor.data

import org.junit._
import Assert._
import java.sql.Connection
import java.sql.Statement
import java.sql.SQLException
import org.apache.log4j.Logger
import org.scalatest.Assertions._

/*
 * Database schema is:
 * create table counter_values(timestamp TIMESTAMP , count BIGINT, element_type VARCHAR(6), "
 * 		+ "bandwidth VARCHAR(15), tweet_length INTEGER, emission_frequency_Hz INTEGER, description VARCHAR(50))
 */

@Test
class RecordsRetrieverTest {

  val _logger:Logger = Logger.getLogger(this.getClass())
  
  val _url1 = "jdbc:derby:test1-db;create=true";
  val _url2 = "jdbc:derby:test2-db;create=true";
  val _createTableStr = "create table counter_values(timestamp TIMESTAMP , count BIGINT, element_type VARCHAR(6), bandwidth VARCHAR(15), tweet_length INTEGER, emission_frequency_Hz INTEGER, description VARCHAR(50))"
  val _deleteTableStr = "delete from counter_values"
    
  def _createTables() = {
    try {
      var conn: Connection = ConnectionProvider.getConnection(_url1)
      var stmt: Statement = conn.createStatement()
      stmt.executeUpdate(_createTableStr)
      conn.commit()
    } catch {
      case sqlEx: SQLException => {
        _logger.warn("Exception during creation of table counter_values for " + _url1 + ": "+sqlEx.toString)
      }
    }
    
    try {
      var conn2: Connection = ConnectionProvider.getConnection(_url2)
      var stmt2: Statement = conn2.createStatement()
      stmt2.executeUpdate(_createTableStr)
      conn2.commit()
    } catch {
      case sqlEx: SQLException => {
        _logger.warn("Exception during creation of table counter_values for " + _url2 + ": " + sqlEx.toString)
      }
    }
  }

  def _prepareDatabase(inserts1: List[String], inserts2: List[String]) = {

    _createTables()
    var conn: Connection = ConnectionProvider.getConnection(_url1)
    var stmt: Statement = conn.createStatement()

    inserts1.foreach(s => stmt.executeUpdate(s))
    conn.commit()

    var conn2: Connection = ConnectionProvider.getConnection(_url2)
    var stmt2: Statement = conn2.createStatement()
    inserts2.foreach(s => stmt2.executeUpdate(s))
    conn2.commit()
  }

  @After
  def _clearDatabase() = {
    try {
      var conn: Connection = ConnectionProvider.getConnection(_url1)
      var stmt: Statement = conn.createStatement()
      stmt.executeUpdate(_deleteTableStr)
      conn.commit()
    } catch {
      case sqlEx: SQLException => _logger.warn("Exception during deletion of table counter_values for "+_url1 + ": " + sqlEx.toString)
    }
    
    try {
      var conn2: Connection = ConnectionProvider.getConnection(_url2)
      var stmt2: Statement = conn2.createStatement()
      stmt2.executeUpdate(_deleteTableStr)
      conn2.commit()
    }  catch {
      case sqlEx: SQLException => _logger.warn("Exception during deletion of table counter_values for "+_url2 + ": " + sqlEx.toString)
    }
  }

  @Test
  def testNonRepeatingRecords() = {
    var l = List("insert into counter_values values('2012-04-05 12:00:05', 5, 'SPOUT', '2Mbit/s', 100, 200, 'some_description')",
        "insert into counter_values values('2012-04-05 12:01:00', 10, 'SPOUT', '2Mbit/s', 100, 200, 'some_description')")
    var l2 = List("insert into counter_values values('2012-04-05 12:00:06', 6, 'BOLT', '2Mbit/s', 100, 200, 'some_description')",
        "insert into counter_values values('2012-04-05 12:01:01', 12, 'BOLT', '2Mbit/s', 100, 200, 'some_description')",
        "insert into counter_values values('2012-04-06 00:00:01', 18, 'BOLT', '2Mbit/s', 100, 200, 'some_description')")
    
    _prepareDatabase(l, l2)

    var retriever = new RecordsRetriever("2Mbit/s", 100, 200) // bw, tweetLength, emissionFreq
    var result = retriever.getSortedRecords(_url1, _url2)
    assertNotNull(result)
    assertResult(5) { result.size }
//    _logger info result(0).toString
//    _logger info result(1).toString
    assert(result(0) equals new DataRecord("2012-04-05 12:00:05.0",valForSpout=5))
    assert(result(1) equals new DataRecord("2012-04-05 12:00:06.0",valForBolt=6))
    assert(result(2) equals new DataRecord("2012-04-05 12:01:00.0",valForSpout=10))
    assert(result(3) equals new DataRecord("2012-04-05 12:01:01.0",valForBolt=12))
    assert(result(4) equals new DataRecord("2012-04-06 00:00:01.0",valForBolt=18))
    
    result.foreach(_logger debug _.toString)

  }
  
  @Test
  def testWithRepeatingRecords() = {
    var l = List("insert into counter_values values('2012-04-05 12:00:05', 5, 'SPOUT', '2Mbit/s', 100, 200, 'some_description')",
        "insert into counter_values values('2012-04-05 12:00:07', 10, 'SPOUT', '2Mbit/s', 100, 200, 'some_description')")
        
    var l2 = List("insert into counter_values values('2012-04-05 12:00:06', 12, 'BOLT', '2Mbit/s', 100, 200, 'some_description')",
        "insert into counter_values values('2012-04-05 12:00:05', 6, 'BOLT', '2Mbit/s', 100, 200, 'some_description')") 
        
   _prepareDatabase(l, l2)
   
   var retriever = new RecordsRetriever("2Mbit/s",100,200) // bw, tweetLength, emissionFreq
   var result = retriever.getSortedRecords(_url1, _url2)
   
   assertNotNull(result)
   assertResult(3) { result.size }
   result.foreach(_logger debug _.toString)
   assert(result(0) equals new DataRecord("2012-04-05 12:00:05.0",valForSpout=5,valForBolt=6))
   assert(result(1) equals new DataRecord("2012-04-05 12:00:06.0",valForBolt=12))
   assert(result(2) equals new DataRecord("2012-04-05 12:00:07.0",valForSpout=10))
  }
}