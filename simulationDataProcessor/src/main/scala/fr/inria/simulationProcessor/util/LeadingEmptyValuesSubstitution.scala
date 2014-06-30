package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord
import org.apache.log4j.Logger

trait LeadingEmptyValuesToZeroSubstitution extends EmptyValuesSubstitution {

  private val _logger: Logger = Logger.getLogger("LeadingEmptyValuesToZeroSubstitution")

  abstract override def substitute(l: List[DataRecord]): List[DataRecord] = {

    def _substituteUsingOperations(op: EmptyValuesSubstitution.ValueOperations)(list: List[DataRecord]): List[DataRecord] = {
      var _leadingEmpty = true
      // let's use the for-comprehensions style, it looks cool ;-)
      for {
        record <- l
      } yield {
        if (_leadingEmpty && op.getter(record) == getEmptyValue) op.setter(record, 0) else { _leadingEmpty = false; record }
      }

      //          l.map((x: DataRecord) => {
      //            if (_leadingEmpty && op.getter(x) == getEmptyValue) op.setter(x, 0) else { _leadingEmpty = false; x }
      //          })

      // iterative one also possible ...
      //      var result = List[DataRecord]()
      //      for (i <- 0 until list.length) {
      //        if (_leadingEmpty && op.getter(list(i)) == getEmptyValue) {
      //          result = result.:+(op.setter(list(i), 0))
      //        } else {
      //          _leadingEmpty = false
      //          result = result.:+(list(i))
      //        }
      //      }
      //      result
    }

    _logger.debug(".substitute")
    def _substituteBoltValues = _substituteUsingOperations(EmptyValuesSubstitution.getBoltOperations)_
    def _substituteSpoutValues = _substituteUsingOperations(EmptyValuesSubstitution.getSpoutOperations)_

    var _l = List(_substituteSpoutValues, _substituteBoltValues).foldLeft(l)((list, fn) => fn(list))

    //    var _l_first = _substituteBoltValues(l) 
    //    _logger.debug("After _substituteBoltValues: "+ _l_first)
    //
    //    var _l = _substituteSpoutValues(_l_first)
    //    _logger.debug("After _subsituteSpoutValues: " + _l)
    //    
    //    _logger.debug("After substitution: " + _l)

    super.substitute(_l)
  }

  abstract override def getEmptyValue = super.getEmptyValue

}