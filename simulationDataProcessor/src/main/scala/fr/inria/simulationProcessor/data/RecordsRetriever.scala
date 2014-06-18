package fr.inria.simulationProcessor.data

import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import scala.collection.mutable.HashMap
import java.util.logging.Logger

/*
 * Database schema is:
 * create table counter_values(timestamp TIMESTAMP , count BIGINT, element_type VARCHAR(6), "
 * 		+ "bandwidth VARCHAR(15), tweet_length INTEGER, emission_frequency_Hz INTEGER, description VARCHAR(50))
 */

class RecordsRetriever(val bandwidth: String, val tweetLength: Int, val emissionFrequency: Int) {

  private val _selectStr: String = "select timestamp, count, element_type from counter_values where bandwidth = '" +
    bandwidth + "' and tweet_length = " + tweetLength + " and emission_frequency_Hz = " + emissionFrequency

  private val TypeSpout = "SPOUT"
  private val TypeBolt = "BOLT"

  private def _isSpout(s: String): Boolean = TypeSpout.equalsIgnoreCase(s)
  private def _isBolt(s: String): Boolean = TypeBolt.equalsIgnoreCase(s)
  
  /**
   * returns a map containing records from associated with the given timestamp
   */
  private def _getRecords(url: String): HashMap[String, DataRecord] = {
    var conn1: Connection = ConnectionProvider(url)
    var readStmt: Statement = conn1.createStatement()
    var rs: ResultSet = readStmt.executeQuery(_selectStr)

    var resultRecords: HashMap[String, DataRecord] = HashMap[String, DataRecord]() // a mutable one - I know it's not fp-style

    while (rs.next()) {
      var ts = rs.getString("timestamp")
      var count = rs.getLong("count")
      var elementType = rs.getString("element_type")

      var record: DataRecord = if (_isSpout(elementType))
        new DataRecord(ts = ts, valForSpout = count)
      else if (_isBolt(elementType))
        new DataRecord(ts = ts, valForBolt = count)
      else
        new DataRecord(ts = ts, valForSpout = -2, valForBolt = -2) // a value only for debugging purposes, maybe:-P

      resultRecords += ((ts, record)) // update the map

    }

    resultRecords
  }

  def getSortedRecords(url_1: String, url_2: String) = {

    def mergeRecordMaps(): HashMap[String, DataRecord] = {
      var res: HashMap[String, DataRecord] = _getRecords(url_1)
      var res2: HashMap[String, DataRecord] = _getRecords(url_2)

      res2.foreach {
        case (k: String, v: DataRecord) => {
          if (!(res contains k)) {
            res += ((k, v))
          } else {
            var rec: DataRecord = res.getOrElse(k, null);
            if (rec.valueForSpout == DataRecord.EmptyRecord) {
              rec.valueForSpout = v.valueForSpout
            } 
            else if (rec.valueForBolt == DataRecord.EmptyRecord) {
              rec.valueForBolt = v.valueForBolt
            }
            res.put(k, rec)
          }
        }
      }

      res
    }

    mergeRecordMaps().values.toList.sortWith(
      (rec1: DataRecord, rec2: DataRecord) => { rec1.timestamp.compareToIgnoreCase(rec2.timestamp) < 0 }) // to sort it in ascending order

  }

}