package agents.both.batch_insert

import testutils.RandomStringCreator


/**
 * Trait for defining batch size for testing purposes
 */
trait TestUtils {
  /**
   * Current testing batch value
   */
  protected val batchSizeVal = 5

  def randomString = RandomStringCreator.randomAlphaString(10)
}
