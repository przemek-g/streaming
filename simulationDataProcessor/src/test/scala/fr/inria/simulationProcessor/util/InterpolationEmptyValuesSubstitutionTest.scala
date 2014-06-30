package fr.inria.simulationProcessor.util

import org.junit._
import Assert._
import org.scalatest.Assertions._
import org.apache.log4j.Logger
import fr.inria.simulationProcessor.data.DataRecord

class InterpolationEmptyValuesSubstitutionTest {

  private val _logger: Logger = Logger.getLogger(this.getClass())

  @Test
  def testWithoutEmptyValues() = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 5),
      new DataRecord("2012-04-05 12:00:05.02", valForSpout = 1, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 7, valForBolt = 10),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 9, valForBolt = 8))

    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution).substitute(l)

    assertResult(6) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 5), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.02", valForSpout = 1, valForBolt = 6), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 7, valForBolt = 10), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 9), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.09", valForSpout = 9, valForBolt = 8), resultList(5))
  }

  @Test
  def testWithEmptyValuesAtBeginningAndEnd = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5),
      new DataRecord("2012-04-05 12:00:05.02", valForSpout = 1),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 7, valForBolt = 10),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 9))

    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution).substitute(l)

    assertResult(6) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 7), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.02", valForSpout = 1, valForBolt = 7), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 7, valForBolt = 10), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 9), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.09", valForSpout = 9, valForBolt = 9), resultList(5))
  }
  
  @Test
  def testWithEmptyBoltValues = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.05", valForSpout = 5, valForBolt = 5),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 6),
      new DataRecord("2012-04-05 12:00:05.10", valForSpout = 10, valForBolt = 10),
      new DataRecord("2012-04-05 12:00:05.12", valForSpout = 12),
      new DataRecord("2012-04-05 12:00:05.20", valForSpout = 18, valForBolt = 30),
      new DataRecord("2012-04-05 12:00:05.24", valForSpout = 16),
      new DataRecord("2012-04-05 12:00:05.55", valForSpout = 17, valForBolt = 44))

    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution).substitute(l)

    assertResult(7) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.05", valForSpout = 5, valForBolt = 5), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 6, valForBolt = 8), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.10", valForSpout = 10, valForBolt = 10), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.12", valForSpout = 12, valForBolt = 14), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.20", valForSpout = 18, valForBolt = 30), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.24", valForSpout = 16, valForBolt = 31), resultList(5)) // 31.6.toLong, exactly
    assertEquals(new DataRecord("2012-04-05 12:00:05.55", valForSpout = 17, valForBolt = 44), resultList(6))
  }
  
  @Test
  def testWithEmptySpoutValues = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.05", valForBolt = 5, valForSpout = 5),
      new DataRecord("2012-04-05 12:00:05.08", valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.10", valForBolt = 10, valForSpout = 10),
      new DataRecord("2012-04-05 12:00:05.12", valForBolt = 12),
      new DataRecord("2012-04-05 12:00:05.20", valForBolt = 18, valForSpout = 30),
      new DataRecord("2012-04-05 12:00:05.24", valForBolt = 16),
      new DataRecord("2012-04-05 12:00:05.55", valForBolt = 17, valForSpout = 44))

    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution).substitute(l)

    assertResult(7) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.05", valForSpout = 5, valForBolt = 5), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 8, valForBolt = 6), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.10", valForSpout = 10, valForBolt = 10), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.12", valForSpout = 14, valForBolt = 12), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.20", valForSpout = 30, valForBolt = 18), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.24", valForSpout = 31, valForBolt = 16), resultList(5)) // 31.6.toLong, exactly
    assertEquals(new DataRecord("2012-04-05 12:00:05.55", valForSpout = 44, valForBolt = 17), resultList(6))
  }
  
  @Test
  def testWithEmptyBoltAndSpoutValues = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.05", valForBolt = 5, valForSpout = 5),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 6),
      new DataRecord("2012-04-05 12:00:05.10", valForBolt = 20, valForSpout = 10),
      new DataRecord("2012-04-05 12:00:05.12", valForBolt = 12),
      new DataRecord("2012-04-05 12:00:05.20", valForBolt = 18, valForSpout = 50),
      new DataRecord("2012-04-05 12:00:05.24", valForBolt = 16),
      new DataRecord("2012-04-05 12:00:05.55", valForBolt = 17, valForSpout = 72))

    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution).substitute(l)

    assertResult(7) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.05", valForSpout = 5, valForBolt = 5), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 6, valForBolt = 14), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.10", valForSpout = 10, valForBolt = 20), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.12", valForSpout = 18, valForBolt = 12), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:05.20", valForSpout = 50, valForBolt = 18), resultList(4))
    assertEquals(new DataRecord("2012-04-05 12:00:05.24", valForSpout = 52, valForBolt = 16), resultList(5)) 
    assertEquals(new DataRecord("2012-04-05 12:00:05.55", valForSpout = 72, valForBolt = 17), resultList(6))
  }
  
  @Test
  def testWithLargerTimeDifferences() = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.05", valForBolt = 5, valForSpout = 5),
      new DataRecord("2012-04-05 12:00:06.08", valForSpout = 6),
      new DataRecord("2012-04-05 12:00:07.10", valForBolt = 20, valForSpout = 10),
      new DataRecord("2012-04-05 12:00:08.12", valForBolt = 12),
      new DataRecord("2012-04-05 12:00:09.08", valForBolt = 18, valForSpout = 50))

    var resultList: List[DataRecord] = (new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord) with InterpolationEmptyValuesSubstitution).substitute(l)

    assertResult(5) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.05", valForSpout = 5, valForBolt = 5), resultList(0))
    assertEquals(new DataRecord("2012-04-05 12:00:06.08", valForSpout = 6, valForBolt = 12), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:07.10", valForSpout = 10, valForBolt = 20), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:08.12", valForSpout = 30, valForBolt = 12), resultList(3))
    assertEquals(new DataRecord("2012-04-05 12:00:09.08", valForSpout = 50, valForBolt = 18), resultList(4))
  }

}