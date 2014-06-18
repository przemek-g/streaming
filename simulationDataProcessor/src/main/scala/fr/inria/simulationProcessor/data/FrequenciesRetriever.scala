package fr.inria.simulationProcessor.data

import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import org.apache.log4j.Logger

class FrequenciesRetriever(var bw:String, var tl:Int) {

  private val _logger:Logger = Logger.getLogger(this.getClass())
  
  def bandwidth = bw
  def tweetLength = tl
  
  private val _selectStr = "select distinct emission_frequency_Hz from counter_values where bandwidth='"+bandwidth+"' and tweet_length="+tweetLength
//  private val _selectStr = "select distinct emission_frequency_Hz from counter_values"
  
  def getDistinctFrequencyValues(url:String) = {
    var conn1: Connection = ConnectionProvider(url)
    var readStmt: Statement = conn1.createStatement()
    var rs: ResultSet = readStmt.executeQuery(_selectStr)
    
    var l = List[Int]()
    
    while (rs.next) {
      l = rs.getInt(1)::l
    }
    
    l
  }
}