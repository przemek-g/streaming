package fr.inria.simulationProcessor

import org.clapper.argot._
import ArgotConverters._
import org.apache.log4j.Logger
import java.io.IOException
import fr.inria.simulationProcessor.data.RecordsRetriever
import fr.inria.simulationProcessor.export.CSVDataRecordsExporter
import fr.inria.simulationProcessor.data.FrequenciesRetriever
import fr.inria.simulationProcessor.util.EmptyValuesNearestSubstitution
import fr.inria.simulationProcessor.data.DataRecord
import fr.inria.simulationProcessor.util.LeadingEmptyValuesToZeroSubstitution
import fr.inria.simulationProcessor.util.LeadingEmptyValuesToZeroSubstitution
import fr.inria.simulationProcessor.util.InterpolationEmptyValuesSubstitution
/**
 * @author Przemek
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

  private val _description = _parser.option[String](List("description", "desc"), "description_string", "The value of the description attribute of the records held in the databases")
  private val _emptyValuesInterpolation = _parser.flag[Boolean](List("interpolate"), "Flag indicating whether to perform linear interpolation of empty (-1) values in data records lists;" +
    "if not specified, substitution with nearest 'neighbour' values will take place")

  private val _emptyRecord = _parser.option[Int](List("emptyRecord"), "value_designating_no_record", "By default it's -1")

  private val _cliOptions = List(_database_1_url, _database_2_url, _fileName, _bandwidth, _tweetLength, _emissionFrequency, _description, _emptyRecord)

  private def _printArgs = {
    var s = new StringBuilder()

    _cliOptions.foreach(option => {
      s append option.name
      s append " : "
      s append option.value.mkString
      s append ", \n"
    })

    println(s.toString)
    println(_emptyValuesInterpolation.value.mkString)
  }

  /**
   * If the emissionFrequency is 0, let's process all frequencies (instead of just one)
   */
  private def _runSimulationDataProcessor() = {

    /* --- define some auxiliary methods --- */
    /* returns a list containing frequencies (unique occurrences) from both databse urls */
    def _getUniqueFrequenciesFromTwoDatabases() = {
      val freqRetriever = new FrequenciesRetriever(_bandwidth.value.get, _tweetLength.value.get)
      val s = freqRetriever.getDistinctFrequencyValues(_database_1_url.value.get).toSet
      s.++(freqRetriever.getDistinctFrequencyValues(_database_2_url.value.get)).toList.sortWith(_ < _)
    }

    /* returns a list containing all the frequencies there are to process (one-element list if there's just one freq to process) */
    def _getFrequenciesToProcess(): List[Int] = {
      _emissionFrequency.value.get match {
        case 0 => _getUniqueFrequenciesFromTwoDatabases
        case _ => List(_emissionFrequency.value.get)
      }
    }

    /* if the given file name contains .csv suffix removes it (it's to be added later) */
    def _adjustFileName(): String = {
      var s: String = _fileName.value.mkString
      if (s.substring(s.length() - 4) equals ".csv") {
        s.substring(0, s.length - 4)
      } else {
        s
      }
    }

    /* if a description was given, return it; otherwise, return an empty string */
    def _getDescription() = if (_description.value != None && _description.value.get.length() > 0) _description.value.get else ""
    /* if a value designating empty records was given, return it; otherwise, return the default empty record value */
    def _getEmptyRecordValue() = if (_emptyRecord.value != None) _emptyRecord.value.get else DataRecord.EmptyRecord

    DataRecord.EmptyRecord = _getEmptyRecordValue()
    val _shouldInterpolate = (_emptyValuesInterpolation.value != None && _emptyValuesInterpolation.value.get)

    /* now, for every frequency there is, let's process it and export a file */
    _getFrequenciesToProcess.foreach(f => {

      _logger info "Processing records for emissionFrequency: " + f
      var retriever = new RecordsRetriever(_bandwidth.value.mkString, _tweetLength.value.get, f, _getDescription())

      var records = retriever.getSortedRecords(_database_1_url.value.mkString, _database_2_url.value.mkString)

      if (records.nonEmpty) {
        var fileName = _adjustFileName + "_" + f + "_Hz.csv"
        _logger info "Saving " + records.size + " records to file: " + fileName
        records = if (_shouldInterpolate)
          (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution with LeadingEmptyValuesToZeroSubstitution).substitute(records)
        else
          (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with LeadingEmptyValuesToZeroSubstitution).substitute(records)
        var exporter = new CSVDataRecordsExporter(fileName)
        exporter export records
      } else {
        _logger info "No records to save for frequency " + f
      }

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
