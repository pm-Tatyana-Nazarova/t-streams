package com.bwsw.tstreams.agents.producer

/**
 * Producer policies for newTransaction method
 */
object ProducerPolicies extends Enumeration {
  type ProducerPolicy = Value

  /**
   * If previous transaction was opened it will be closed
   */
  val checkpointIfOpen = Value

  /**
   * If previous transaction was opened it will be canceled
   */
  val cancelIfOpen = Value

  /**
   * If previous transaction was opened exception will be thrown
   */
  val errorIfOpen = Value
}
