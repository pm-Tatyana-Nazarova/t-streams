package com.bwsw.tstreams.utils

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs

/**
 * Entity for generating new transaction time
 */
class GeneratorsEntity {
  /**
   * @return Transaction UUID
   */
  def getTimeUUID(): UUID = UUIDs.timeBased()
}