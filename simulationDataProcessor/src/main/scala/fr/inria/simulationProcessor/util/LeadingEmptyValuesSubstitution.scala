package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord

trait LeadingEmptyValuesToZeroSubstitution extends EmptyValuesSubstitution {

  private def _substitute(l: List[DataRecord], op: EmptyValuesSubstitution.ValueOperations): List[DataRecord] = {
    var _trailingEmpty = true

    l.map((x: DataRecord) => {
      if (_trailingEmpty && op.getter(x) == super.getEmptyValue) op.setter(x, 0) else { _trailingEmpty = false; x }
    })
  }

  abstract override def substitute(l: List[DataRecord]): List[DataRecord] = {
    var _l = List(EmptyValuesSubstitution.getSpoutOperations, EmptyValuesSubstitution.getBoltOperations)
      .foldLeft(l)((l, operations) => _substitute(l, operations))
    super.substitute(_l)
  }

  abstract override def getEmptyValue = super.getEmptyValue

}