package fr.inria.simulationProcessor.export

import org.junit._
import Assert._
import org.scalatest.Assertions._
import fr.inria.simulationProcessor.data.DataRecord
import org.apache.log4j.Logger
import java.io.File

@Test
class CSVDataRecordsExporterTest {

  private var _logger: Logger = Logger.getLogger(this.getClass())

  private val _fileName = "testfile.csv"
  
  @After
  def removeFile() : Unit = {
    new File(_fileName).delete
  }
  
  @Test
  def testWithOneDataRecord() = {
    var exporter = new CSVDataRecordsExporter(_fileName)
    var l = List(new DataRecord("2013-04-07 10:04:03.0", valForSpout = 5, valForBolt = 6))
    exporter.export(l)

    var source = scala.io.Source.fromFile(_fileName)
    assertNotNull(source)

    var s: String = source.mkString
    _logger debug s
    assertEquals("2013-04-07 10:04:03.0;5;6;\n", s)

    source.close
  }

  @Test
  def testWithNoDataRecords() = {
    var exporter = new CSVDataRecordsExporter(_fileName)
    exporter.export(List())

    var source = scala.io.Source.fromFile(_fileName)
    assertNotNull(source)
    var s: String = source.mkString
    _logger debug s
    assertEquals("", s)

    source.close
  }

  @Test
  def testWithMultipleDataRecords() = {
    var exporter = new CSVDataRecordsExporter(_fileName)
    exporter.export(
      List(
        new DataRecord("2013-04-07 10:04:03.0", valForSpout = 5, valForBolt = 6),
        new DataRecord("2013-04-07 11:05:04.0", valForSpout = 7, valForBolt = 8),
        new DataRecord("2013-04-07 12:06:05.0", valForSpout = 9, valForBolt = 10)))

    var source = scala.io.Source.fromFile(_fileName)
    assertNotNull(source)
    var s = source.mkString
    assertEquals("2013-04-07 10:04:03.0;5;6;\n2013-04-07 11:05:04.0;7;8;\n2013-04-07 12:06:05.0;9;10;\n", s)
    
    source.close
  }
}