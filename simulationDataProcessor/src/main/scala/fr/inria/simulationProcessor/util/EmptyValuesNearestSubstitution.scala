package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord

class EmptyValuesNearestSubstitution(var emptyVal:Long) extends EmptyValuesSubstitution {

  def emptyValue = emptyVal
  
  def substitute(l:List[DataRecord]) : List[DataRecord] = {
    
		  def _getSpoutValue(rec:DataRecord):Long = rec.valueForSpout
		  def _getBoltValue(rec:DataRecord):Long = rec.valueForBolt
		  
		  def _setSpoutValue(rec:DataRecord,newVal:Long) : DataRecord = { rec.valueForSpout = newVal; rec }
		  def _setBoltValue(rec:DataRecord,newVal:Long) : DataRecord = { rec.valueForBolt = newVal; rec }
		 
    trait ValueOperations {
      def getter : (DataRecord)=>Long
      def setter : (DataRecord,Long)=>DataRecord
    }
		  
    class BoltValueOperations extends ValueOperations {
      def getter = _getBoltValue
      def setter = _setBoltValue
    }
    
    class SpoutValueOperations extends ValueOperations {
      def getter = _getSpoutValue
      def setter = _setSpoutValue
    }
    
    def _substituteUsingOperations(ops:ValueOperations)(l:List[DataRecord]) : List[DataRecord] = {
      
      def _findNearestNonEmpty(rec:DataRecord) : DataRecord = {
        var laterAndNonEmptyElements:List[DataRecord] = l.filter((r) => { ops.getter(r) != emptyValue && r.timestamp > rec.timestamp })
        if (laterAndNonEmptyElements.nonEmpty) laterAndNonEmptyElements(0)
        else {
          var earlierAndNonEmptyElements:List[DataRecord] = l.filter((r)=> { ops.getter(r) != emptyValue && r.timestamp < rec.timestamp })
          if (earlierAndNonEmptyElements.nonEmpty) earlierAndNonEmptyElements.last else null
        }
      }
      
     l.map((rec:DataRecord) => {
        if (ops.getter(rec) != emptyValue) rec
        else {
          var nonEmptyRec = _findNearestNonEmpty(rec)
          if (nonEmptyRec != null) ops.setter(rec,ops.getter(nonEmptyRec)) //substitute the value
          else rec
        }
      })
    }
    
    def _substituteBoltValues = _substituteUsingOperations(new BoltValueOperations())_
    def _substituteSpoutValues = _substituteUsingOperations(new SpoutValueOperations())_
    List(_substituteBoltValues, _substituteSpoutValues).foldLeft(l)((list,fn) => fn(list))
  } 
  // end method substitute
}