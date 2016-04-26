package testutils

import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator

/**
 * Helper object for creating LocalTimeTxnGenerator
 */
object LocalGeneratorCreator {
  def getGen() = new LocalTimeUUIDGenerator
}