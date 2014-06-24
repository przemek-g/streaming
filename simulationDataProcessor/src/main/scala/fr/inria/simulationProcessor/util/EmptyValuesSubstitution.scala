package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord

abstract class EmptyValuesSubstitution {

  /* Some helper classes to abstract off the choice of a DataRecord's methods to use */
  protected trait ValueOperations {
    def getter: (DataRecord) => Long
    def setter: (DataRecord, Long) => DataRecord
  }

  protected class BoltValueOperations extends ValueOperations {
    def getter = (rec: DataRecord) => rec.valueForBolt
    def setter = (rec: DataRecord, newVal: Long) => { rec.valueForBolt = newVal; rec }
  }

  protected class SpoutValueOperations extends ValueOperations {
    def getter = (rec: DataRecord) => rec.valueForSpout
    def setter = (rec: DataRecord, newVal: Long) => { rec.valueForSpout = newVal; rec }
  }

  /* the abstract methods */
  def substitute(l: List[DataRecord]): List[DataRecord]
  def getEmptyValue : Long
}