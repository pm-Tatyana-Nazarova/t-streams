package com.bwsw.tstreams.interaction.transport.traits

import java.util.UUID

import com.bwsw.tstreams.interaction.PeerToPeerAgent

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
