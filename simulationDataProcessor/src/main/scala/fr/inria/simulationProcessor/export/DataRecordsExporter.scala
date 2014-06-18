package fr.inria.simulationProcessor.export

import fr.inria.simulationProcessor.data.DataRecord

/**
 * An interface for exporting series of records
 */
trait DataRecordsExporter {

  def export(records:List[DataRecord])
}

trait FileDataRecordsExporter extends DataRecordsExporter {
  
  def open()
  def close()
  def writeRecord(record:DataRecord)
  
  override def export(records:List[DataRecord]) = {
    open()
    records.foreach( writeRecord(_) )
    close()
  }
}