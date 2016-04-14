package com.bwsw.tstreams.generator

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs

/**
 * Entity for generating new transaction time UUID
 */
class LocalTimeUuidGenerator extends IuuidGenerator{
  /**
   * @return Transaction UUID
   */
  def getTimeUUID(): UUID = UUIDs.timeBased()
}