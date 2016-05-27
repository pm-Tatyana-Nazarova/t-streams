package com.bwsw.tstreams.coordination.transactions.transport.traits

import java.util.UUID

import com.bwsw.tstreams.coordination.transactions.PeerToPeerAgent

/**
 * Trait for producers
 */
trait Interaction {
  /**
   * Method to implement for concrete producer
   * Need only if this producer is master
   * @return UUID
   */
  def getLocalTxn(partition : Int) : UUID

  /**
   * Agent for producer to provide producers communication
   */
  val agent : PeerToPeerAgent
}
