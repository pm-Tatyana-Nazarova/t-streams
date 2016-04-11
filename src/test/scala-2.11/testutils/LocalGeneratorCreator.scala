package testutils

import com.bwsw.tstreams.utils.LocalTimeTxnGenerator

/**
 * Helper object for creating LocalTimeTxnGenerator
 */
object LocalGeneratorCreator {
  def getGen() = new LocalTimeTxnGenerator
}
