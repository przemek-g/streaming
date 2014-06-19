package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord

trait EmptyValuesSubstitution {

  def substitute(l:List[DataRecord]) : List[DataRecord]
}