package com.bwsw.tstreams.newcoordination.subscribe.messages

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
   * If transaction cancelled
   */
  val cancelled = Value

}

