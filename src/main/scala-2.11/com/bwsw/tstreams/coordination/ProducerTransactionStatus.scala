package com.bwsw.tstreams.coordination

/**
 * Status for producer topic messages
 */
object ProducerTransactionStatus extends Enumeration {
  type ProducerTransactionStatus = Value

  /**
   * If transaction opened
   */
  val opened = Value

  /**
   * If transaction closed
   */
  val closed = Value

  /**
   * If transaction canceled
   */
  val canceled = Value
}

