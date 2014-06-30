package fr.inria.simulationProcessor.util

import org.junit._
import Assert._
import org.scalatest.Assertions._
import org.apache.log4j.Logger
import fr.inria.simulationProcessor.data.DataRecord

class LeadingEmptyValuesToZeroSubstitutionTest {
  
  @Test
  def testForBolt() = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5),
      new DataRecord("2012-04-05 12:00:05.02", valForSpout = 1),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 7),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 9))
      
    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with LeadingEmptyValuesToZeroSubstitution).substitute(l)
        
    assertResult(6) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 0), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.02", valForSpout = 1, valForBolt = 0), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 7, valForBolt = 7), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 9), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.09", valForSpout = 9, valForBolt = 9), resultList(5))
  }
  
  @Test
  def testForSpout() = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForBolt = 5),
      new DataRecord("2012-04-05 12:00:05.02", valForBolt = 1),
      new DataRecord("2012-04-05 12:00:05.06", valForBolt = 6, valForSpout = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.08", valForBolt = 8, valForSpout = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForBolt = 9))
      
    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with LeadingEmptyValuesToZeroSubstitution).substitute(l)
        
    assertResult(6) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.01", valForBolt = 5, valForSpout = 0), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.02", valForBolt = 1, valForSpout = 0 ), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.06", valForBolt = 6, valForSpout = 7), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForBolt = 7, valForSpout = 9), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForBolt = 8, valForSpout = 9), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.09", valForBolt = 9, valForSpout = 9), resultList(5))
  }
  
}