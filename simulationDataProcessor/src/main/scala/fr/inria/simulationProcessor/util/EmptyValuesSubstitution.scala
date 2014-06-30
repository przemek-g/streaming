package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord

abstract class EmptyValuesSubstitution {

  /* the abstract methods */
  def substitute(l: List[DataRecord]): List[DataRecord]
  def getEmptyValue: Long
}

object EmptyValuesSubstitution {
  /* Some helper classes to abstract off the choice of a DataRecord's methods to use */
  trait ValueOperations {
    def getter: (DataRecord) => Long
    def setter: (DataRecord, Long) => DataRecord
    def compare: (DataRecord, DataRecord) => Boolean
    def chooseSubstitute: (List[DataRecord]) => DataRecord
    def chooseSecondarySubstitute: (List[DataRecord]) => DataRecord
  }

  class BoltValueOperations extends ValueOperations {
    def getter = (rec: DataRecord) => rec.valueForBolt
    def setter = (rec: DataRecord, newVal: Long) => { rec.valueForBolt = newVal; rec }

    /* for a bolt we want to choose the last of the bolts that are earlier than this one*/
    def compare = (pattern: DataRecord, obj: DataRecord) => obj.timestamp <= pattern.timestamp
    def chooseSubstitute = (l: List[DataRecord]) => l.last
    def chooseSecondarySubstitute = (l: List[DataRecord]) => l(0)
  }

  class SpoutValueOperations extends ValueOperations {
    def getter = (rec: DataRecord) => rec.valueForSpout
    def setter = (rec: DataRecord, newVal: Long) => { rec.valueForSpout = newVal; rec }

    /* for a spout we want to choose the first of the spouts that are later than this one */
    def compare = (pattern: DataRecord, obj: DataRecord) => pattern.timestamp <= obj.timestamp
    def chooseSubstitute = (l: List[DataRecord]) => l(0)
    def chooseSecondarySubstitute = (l: List[DataRecord]) => l.last
  }

  def getBoltOperations() = new BoltValueOperations()

  def getSpoutOperations() = new SpoutValueOperations()
}