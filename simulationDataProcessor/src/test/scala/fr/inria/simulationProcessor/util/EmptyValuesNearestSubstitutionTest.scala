package fr.inria.simulationProcessor.util

import org.junit._
import Assert._
import org.scalatest.Assertions._
import org.apache.log4j.Logger
import fr.inria.simulationProcessor.data.DataRecord

class EmptyValuesNearestSubstitutionTest {

  private val _logger = Logger.getLogger(this.getClass())

  @Test
  def testWithoutEmptyValues() = {
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8, valForBolt = 9))

    var resultList: List[DataRecord] = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(3) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(l(2), resultList(2))
  }

  @Test
  def testWithEmptyList() = {
    var l: List[DataRecord] = List()
    var resultList: List[DataRecord] = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(0) { resultList.size }
  }

  @Test
  def testWithEmptyBoltValues() = {
    /* find non-empty value after the empty one */
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8, valForBolt = 9))

    var resultList: List[DataRecord] = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(3) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 7), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(l(2), resultList(2))

    /* find non-empty value before the empty one */
    l = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8))

    resultList = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(3) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8, valForBolt = 7), resultList(2))

    /* a few empty values in the middle of the list */
    l = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 10, valForBolt = 10))

    resultList = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(5) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8, valForBolt = 10), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 9, valForBolt = 10), resultList(3))
    assertEquals(l(4), resultList(4))
  }

  @Test
  def testWithEmptySpoutValues() = {
    /* find non-empty value after the empty one */
    var l: List[DataRecord] = List(
      new DataRecord("2012-04-05 12:00:05.01", valForBolt = 5),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8, valForBolt = 9))

    var resultList: List[DataRecord] = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(3) { resultList.size }
    assertEquals(new DataRecord("2012-04-05 12:00:05.01", valForSpout = 6, valForBolt = 5), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(l(2), resultList(2))

    /* find non-empty value before the empty one */
    l = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForBolt = 8))

    resultList = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(3) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 6, valForBolt = 8), resultList(2))

    /* a few empty values in the middle of the list */
    l = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForBolt = 8),
      new DataRecord("2012-04-05 12:00:05.08", valForBolt = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 10, valForBolt = 10))

    resultList = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(5) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 10, valForBolt = 8), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 10, valForBolt = 9), resultList(3))
    assertEquals(l(4), resultList(4))
  }
  
  @Test
  def testEmptySpoutAndBoltValues() = {
    var l = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5, valForBolt = 6),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6, valForBolt = 7),
      new DataRecord("2012-04-05 12:00:05.07", valForBolt = 8),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 10, valForBolt = 10))

    var resultList = new EmptyValuesNearestSubstitution(-1).substitute(l)
    assertNotNull(resultList)
    assertResult(5) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(new DataRecord("2012-04-05 12:00:05.07", valForSpout = 9, valForBolt = 8), resultList(2))
    assertEquals(new DataRecord("2012-04-05 12:00:05.08", valForSpout = 9, valForBolt = 10), resultList(3))
    assertEquals(l(4), resultList(4))
  }
  
  @Test
  def testWithoutNonEmptyValues() = {
    var l = List(
      new DataRecord("2012-04-05 12:00:05.01", valForSpout = 5),
      new DataRecord("2012-04-05 12:00:05.06", valForSpout = 6),
      new DataRecord("2012-04-05 12:00:05.07", valForSpout = 8),
      new DataRecord("2012-04-05 12:00:05.08", valForSpout = 9),
      new DataRecord("2012-04-05 12:00:05.09", valForSpout = 10))

    var resultList = new EmptyValuesNearestSubstitution(DataRecord.EmptyRecord).substitute(l)
    assertNotNull(resultList)
    assertResult(5) { resultList.size }
    assertEquals(l(0), resultList(0))
    assertEquals(l(1), resultList(1))
    assertEquals(l(2), resultList(2))
    assertEquals(l(3), resultList(3))
    assertEquals(l(4), resultList(4))
  }
}