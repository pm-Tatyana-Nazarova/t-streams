package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.policy.AbstractPolicy

/**
 * Class for Basic Producer Options
 * @param transactionTTL Single transaction live time
 * @param transactionKeepAliveInterval Update transaction interval which is used to keep alive transaction in time when it is opened
 * @param producerKeepAliveInterval Update producer interval for updating producer info
 * @param writePolicy Strategy for selecting next partition
 * @param converter User defined or basic converter for converting USERTYPE objects to DATATYPE objects(storage object type)
 * @tparam USERTYPE User object type
 * @tparam DATATYPE Storage object type
 */
class BasicProducerOptions[USERTYPE,DATATYPE](val transactionTTL : Int,
                                              val transactionKeepAliveInterval : Int,
                                              val producerKeepAliveInterval : Int,
                                              val writePolicy : AbstractPolicy,
                                              val converter : IConverter[USERTYPE,DATATYPE]) {

  /**
   * Transaction minimum ttl time
   */
    private val minTxnTTL = 3

  /**
   * Options validating
   */
    if (transactionTTL < minTxnTTL)
      throw new IllegalArgumentException(s"transactionTTL should be greater or equal than $minTxnTTL")

    if (transactionKeepAliveInterval < 1)
      throw new IllegalArgumentException(s"transactionKeepAliveInterval should be greater or equal than 1")

    if (transactionKeepAliveInterval.toDouble > transactionTTL.toDouble / 3.0)
      throw new IllegalArgumentException("transactionTTL should be three times greater than transaction")

    if (producerKeepAliveInterval < 1)
      throw new IllegalArgumentException("producerKeepAlive interval should be greater or equal than 1")
}
