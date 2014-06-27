package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord

class EmptyValuesNearestSubstitution(var emptyVal: Long) extends EmptyValuesSubstitution {

  override def getEmptyValue = emptyVal

  override def substitute(l: List[DataRecord]): List[DataRecord] = {

    def _substituteUsingOperations(ops: EmptyValuesSubstitution.ValueOperations)(l: List[DataRecord]): List[DataRecord] = {

      def _findNearestNonEmpty(rec: DataRecord): DataRecord = {
        var firstSearchSubstitutes: List[DataRecord] = l.filter((r) => { ops.getter(r) != getEmptyValue && ops.compare(rec,r) })
        if (firstSearchSubstitutes.nonEmpty) ops.chooseSubstitute(firstSearchSubstitutes)
        else {
          var secondSearchSubstitutes: List[DataRecord] = l.filter((r) => { ops.getter(r) != getEmptyValue && !ops.compare(rec, r) })
          if (secondSearchSubstitutes.nonEmpty) ops.chooseSecondarySubstitute(secondSearchSubstitutes) else null
        }
      }

      l.map((rec: DataRecord) => {
        if (ops.getter(rec) != getEmptyValue) rec
        else {
          var nonEmptyRec = _findNearestNonEmpty(rec)
          if (nonEmptyRec != null) ops.setter(rec, ops.getter(nonEmptyRec)) //substitute the value
          else rec
        }
      })
    }

    def _substituteBoltValues = _substituteUsingOperations(EmptyValuesSubstitution.getBoltOperations)_
    def _substituteSpoutValues = _substituteUsingOperations(EmptyValuesSubstitution.getSpoutOperations)_
    List(_substituteSpoutValues, _substituteBoltValues).foldLeft(l)((list, fn) => fn(list))
  }
  // end method substitute
}