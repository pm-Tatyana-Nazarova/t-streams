package com.bwsw.tstreams.generator

import java.util.UUID

/**
 * Trait for producer/consumer transaction unique UUID generating
 */
trait IuuidGenerator {
  /**
   * @return Txn UUID
   */
  def getTimeUUID() : UUID
}
