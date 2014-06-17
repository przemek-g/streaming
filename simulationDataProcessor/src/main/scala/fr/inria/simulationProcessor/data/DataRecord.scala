package fr.inria.simulationProcessor.data

class DataRecord(var ts:String, var valForSpout:Long = -1, var valForBolt:Long = -1) {

  private var _timestamp = ts
  private var _valueForSpout = valForSpout 
  private var _valueForBolt = valForBolt
  
  def timestamp : String = _timestamp 
  
  def valueForSpout : Long = _valueForSpout 
  def valueForSpout_= (v:Long):Unit = _valueForSpout = v // special syntax for a setter
  
  def valueForBolt = _valueForBolt
  def valueForBolt_= (v:Long):Unit = _valueForBolt = v
  
  override def equals(obj:Any):Boolean = obj match {
    case that:DataRecord => that.timestamp.equals(timestamp) && that.valueForBolt==valueForBolt && that.valueForSpout==valueForSpout
    case _ => false
  }
  
  override def toString() = "<timestamp: "+_timestamp+", valueForSpout:"+_valueForSpout+", valueForBolt: "+_valueForBolt+">"
  
}

object DataRecord {
  
  private val _EmptyRecord:Int = -1
  
  def EmptyRecord:Long = _EmptyRecord // getter for the constant 
}