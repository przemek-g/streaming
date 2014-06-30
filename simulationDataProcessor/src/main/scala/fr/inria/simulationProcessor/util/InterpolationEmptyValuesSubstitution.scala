package fr.inria.simulationProcessor.util

import fr.inria.simulationProcessor.data.DataRecord
import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.Interval

trait InterpolationEmptyValuesSubstitution extends EmptyValuesSubstitution {

  private val _logger: Logger = Logger.getLogger("InterpolationEmptyValuesToZeroSubstitution")

  abstract override def substitute(l: List[DataRecord]): List[DataRecord] = {

    def _substituteUsingOperations(op: EmptyValuesSubstitution.ValueOperations)(list: List[DataRecord]): List[DataRecord] = {

      def _interpolate(record: DataRecord): DataRecord = {
        // linear interpolation of value y1 = x1/x2 * y2

        def _findRecNextTo(cmp: (DataRecord, DataRecord) => Boolean, choose: List[DataRecord] => DataRecord)(record: DataRecord): DataRecord = {
          var _compliantElements = list.filter(x => cmp(record, x))
          var result:DataRecord = null
          try {
        	  result = choose(_compliantElements)
          } catch {
            case outOfBounds:IndexOutOfBoundsException => _logger.debug("List has no element at the requested index; "+outOfBounds.toString)
            case noSuchElem:NoSuchElementException => _logger.debug("There is no such element of the list; "+noSuchElem.toString)
            case e:Exception => _logger.debug("Exception while retrieving requested element from the list; "+e.toString)
          }
          result
        }
        // --- end _findRecNextTo ---
        
        def _findPrevious = _findRecNextTo((a: DataRecord, b: DataRecord) => a.timestamp > b.timestamp && op.getter(b) != getEmptyValue, (l: List[DataRecord]) => l.last)_
        def _findNext = _findRecNextTo((a: DataRecord, b: DataRecord) => a.timestamp < b.timestamp && op.getter(b) != getEmptyValue, (l: List[DataRecord]) => l(0))_
        
        def _getTimeDifferenceMillis(a:DataRecord, b:DataRecord) : Long = {
          var formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
          var dateTimeA:DateTime = formatter.parseDateTime(a.timestamp)
          var dateTimeB:DateTime = formatter.parseDateTime(b.timestamp)
          new Interval(dateTimeA, dateTimeB).toDuration().getMillis() // returns the Duration expressed in milliseconds
        }
        // --- end _getTimeDifferenceMillis ---
        
        
        // if either of the two records is not found (i.e. we're at the first or last element) - return the record itself
        var previousRecord = _findPrevious(record)
        if (previousRecord == null) { 
          _logger.debug("Previous record is null: "+previousRecord)
          return record
        }
        _logger.debug("Going on, with previousRecord = "+previousRecord)
        
        var nextRecord = _findNext(record)
        if (nextRecord == null) {
          _logger.debug("Next record is null: "+nextRecord)
          return record
        }
        _logger.debug("Going on, with nextRecord = "+nextRecord)
        
        var x1 =  _getTimeDifferenceMillis(previousRecord,record).toFloat // the shorter segment on the x axis
        var x2 = _getTimeDifferenceMillis(previousRecord, nextRecord).toFloat // the longer segment on the x axis
        var y2 = (op.getter(nextRecord)-op.getter(previousRecord)).toFloat // the longer segment on the y axis
        var interpolatedDifference = y2*x1/x2
        
        _logger.debug("The interpolated difference equals: "+interpolatedDifference+"; the resulting value is therefore: "+(op.getter(previousRecord)+interpolatedDifference.toLong))
        
        op.setter(record,op.getter(previousRecord) + interpolatedDifference.toLong)
      }
      // -- end interpolate --

      list.map((record: DataRecord) => {
        if (op.getter(record) != getEmptyValue) record
        else _interpolate(record) 
      })
    }
    // - end substituteUsingOperations -

    _logger.debug(".substitute")
    def _substituteBoltValues = _substituteUsingOperations(EmptyValuesSubstitution.getBoltOperations)_
    def _substituteSpoutValues = _substituteUsingOperations(EmptyValuesSubstitution.getSpoutOperations)_

    var _l = List(_substituteSpoutValues, _substituteBoltValues).foldLeft(l)((list, fn) => fn(list))

    super.substitute(_l)
  }

  abstract override def getEmptyValue = super.getEmptyValue

}