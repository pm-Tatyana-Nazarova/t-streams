package testutils

import com.bwsw.tstreams.txngenerator.LocalTimeTxnGenerator

/**
 * Helper object for creating LocalTimeTxnGenerator
 */
object LocalGeneratorCreator {
  def getGen() = new LocalTimeTxnGenerator
}
