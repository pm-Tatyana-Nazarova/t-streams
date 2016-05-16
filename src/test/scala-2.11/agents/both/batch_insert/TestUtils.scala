package agents.both.batch_insert

import java.util.UUID
import testutils.RandomStringCreator

import scala.collection.mutable.ListBuffer


/**
 * Trait for defining batch size for testing purposes
 */
trait TestUtils {
  /**
   * Current testing batch value
   */
  protected val batchSizeVal = 5

  def randomString = RandomStringCreator.randomAlphaString(10)

  def isSorted(list : ListBuffer[UUID]) : Boolean = {
    if (list.isEmpty)
      return true
    var checkVal = true
    var curVal = list.head
    list foreach { el =>
      if (el.timestamp() < curVal.timestamp())
        checkVal = false
      if (el.timestamp() > curVal.timestamp())
        curVal = el
    }
    checkVal
  }
}
