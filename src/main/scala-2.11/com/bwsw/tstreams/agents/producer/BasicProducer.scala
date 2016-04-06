package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.streams.BasicStream
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
 * Basic producer class
 * @param name Producer name
 * @param stream Stream for transaction sending
 * @param producerOptions This producer options
 * @tparam USERTYPE User data type
 * @tparam DATATYPE Storage data type
 */
class BasicProducer[USERTYPE,DATATYPE](val name : String,
                                       val stream : BasicStream[DATATYPE],
                                       val producerOptions: BasicProducerOptions[USERTYPE,DATATYPE]) {

  /**
   * BasicProducer logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

  /**
   * Current transaction instance for checking it state
   */
  private var currentTransaction : BasicProducerTransaction[USERTYPE,DATATYPE] = null


  /**
   *
   * @param checkpointPrev If true -> previous transaction will be closed automatically,
   *                       if false -> previous transaction will not be closed automatically and if it is not closed by user,
   *                       exception will be thrown
   * @return BasicProducerTransaction instance
   */
  def newTransaction(checkpointPrev : Boolean) : BasicProducerTransaction[USERTYPE,DATATYPE] = {
    logger.info(s"Start new BasicProducerTransaction for BasicProducer " +
      s"with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

    if (checkpointPrev)
      if (currentTransaction!=null && !currentTransaction.isClosed)
        currentTransaction.close()

    if (currentTransaction!=null && !currentTransaction.isClosed)
      throw new IllegalStateException("current transaction is not closed")

    val transaction = new BasicProducerTransaction[USERTYPE,DATATYPE](
      producerOptions.writePolicy.getNextPartition,
      this)

    currentTransaction = transaction
    transaction
  }
}
