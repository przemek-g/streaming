package fr.inria.simulationProcessor.export

import fr.inria.simulationProcessor.data.DataRecord
import java.io.FileWriter
import java.io.File
import java.io.PrintWriter
import java.io.Writer

class CSVDataRecordsExporter(file:String) extends FileDataRecordsExporter {

  private val _fileName = file
  val fileName = _fileName
  
  private var _writer:PrintWriter = null
  
  override def open = _writer = new PrintWriter(new File(fileName))
  
  override def close = _writer.close
  
  override def writeRecord(record:DataRecord) = {
    var s:StringBuilder = new StringBuilder(record.timestamp)
    s append ";"
    s append record.valueForSpout
    s append ";"
    s append record.valueForBolt
    s append ";"
    _writer println s.toString
  }
}