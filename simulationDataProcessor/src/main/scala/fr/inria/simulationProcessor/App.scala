package fr.inria.simulationProcessor

import org.clapper.argot._
import ArgotConverters._
import org.apache.log4j.Logger
import java.io.IOException
import fr.inria.simulationProcessor.data.RecordsRetriever
import fr.inria.simulationProcessor.export.CSVDataRecordsExporter
import fr.inria.simulationProcessor.data.FrequenciesRetriever
/**
 * @author ${user.name}
 * The main object that parses the command line and runs the tool.
 */
object App {

  private val _logger: Logger = Logger.getLogger(this.getClass())

  private val _parser = new ArgotParser("Simulation data processing tool", preUsage = Some("Version 1.0"), postUsage = Some("Enjoy responsibly! :-D"))

  private val _database_1_url = _parser.option[String](List("db1", "database-1"), "db-name", "Url of the first databse")
  private val _database_2_url = _parser.option[String](List("db2", "database-2"), "db-name", "Url of the second database")
  private val _fileName = _parser.option[String](List("outputFile"), "file-name", "Name of the output csv file to which the data should be exported")

  private val _bandwidth = _parser.option[String](List("bandwidth"), "link-bandwidth", "The string describing network link bandwidth, e.g. 2Mbit/s, 400 Kbit/s, etc.")
  private val _tweetLength = _parser.option[Int](List("tweetLength"), "tweet-length", "The length of a single tweet-message that was the transmission grain in our simulation")
  private val _emissionFrequency = _parser.option[Int](List("emissionFrequency"), "frequency", "The frequency of spouts' emission into the network link (into the topology, more generally)")

  private val _cliOptions = List(_database_1_url, _database_2_url, _fileName, _bandwidth, _tweetLength, _emissionFrequency)

  private def _printArgs = {
    var s = new StringBuilder()

    _cliOptions.foreach(option => {
      s append option.name
      s append " : "
      s append option.value.mkString
      s append ", \n"
    })

    println(s.toString)
  }

  /**
   * If the emissionFrequency is 0, let's process all frequencies (instead of just one)
   */
  private def _runSimulationDataProcessor() = {
    
    def _getUniqueFrequenciesFromTwoDatabases() = {
      val freqRetriever = new FrequenciesRetriever(_bandwidth.value.get, _tweetLength.value.get) 
      val s = freqRetriever.getDistinctFrequencyValues(_database_1_url.value.get).toSet
      s.++(freqRetriever.getDistinctFrequencyValues(_database_2_url.value.get)).toList.sortWith(_ < _)
    }
    
    def _getFrequenciesToProcess() : List[Int] = {
      _emissionFrequency.value.get match {
        case 0 => _getUniqueFrequenciesFromTwoDatabases
        case _ => List(_emissionFrequency.value.get)
      }
    }

    def _adjustFileName() : String = {
      var s:String = _fileName.value.mkString
      if (s.substring(s.length()-4) equals ".csv") {
        s.substring(0,s.length-4)
      }
      else {
        s
      }
    }
    
    _getFrequenciesToProcess.foreach(f => {

      _logger info "Processing records for emissionFrequency: "+f
      var retriever = new RecordsRetriever(_bandwidth.value.mkString, _tweetLength.value.get, _emissionFrequency.value.get)
      var records = retriever.getSortedRecords(_database_1_url.value.mkString, _database_2_url.value.mkString)
      
      var fileName = _adjustFileName + "_" + f + "_Hz.csv"
      _logger info "Saving to file: "+fileName
      var exporter = new CSVDataRecordsExporter(fileName)
      exporter export records
    })

  }

  def main(args: Array[String]) {
    try {
      _parser parse args
      _printArgs
      _runSimulationDataProcessor
    } catch {
      case e: ArgotUsageException => _logger error "Bad Argot CLI parsing usage: " + e.toString
      case e: IOException => _logger error "I/O exception occurred: " + e.toString
      case e: Exception => _logger error "An exception occurred: " + e.toString
    }
  }

}
