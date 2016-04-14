package testutils

import com.bwsw.tstreams.generator.LocalTimeUuidGenerator

/**
 * Helper object for creating LocalTimeTxnGenerator
 */
object LocalGeneratorCreator {
  def getGen() = new LocalTimeUuidGenerator
}