package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicBoolean
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue

/**
 * Basic consumer with subscribe option
 * @param name Name of consumer
 * @param stream Stream from which to consume transactions
 * @param options Basic consumer options
 * @param persistentQueuePath Local Path to queue which maintain transactions that already exist and new incoming transactions
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
//TODO add logging
class BasicSubscribingConsumer[DATATYPE, USERTYPE](name : String,
                                                     stream : BasicStream[DATATYPE],
                                                     options : BasicConsumerOptions[DATATYPE,USERTYPE],
                                                     callBack : BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                     persistentQueuePath : String)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options){

  /**
   * Current subscriber state
   */
  private var isStarted = false

  /**
   * Indicate active subscriber or not
   */
  private val isQueueConsumed = new AtomicBoolean(false)

  /**
   * Start to consume messages
   */
  def start() = {
    if (isStarted)
      throw new IllegalStateException("subscriber already started")

    isStarted = true

    (0 until stream.getPartitions) foreach { partition =>
      //getting last txn for concrete partition
      val lastTransactionOpt = getLastTransaction(partition)

      //creating queue for concrete partition
      val queue =
        if (lastTransactionOpt.isDefined) {
          val txnUuid = lastTransactionOpt.get.getTxnUUID
          new PersistentTransactionQueue(persistentQueuePath + s"/$partition", txnUuid)
        }
        else {
          new PersistentTransactionQueue(persistentQueuePath + s"/$partition", null)
        }

      val transactionRelay = new SubscriberTransactionsRelay(this, currentOffsets(partition), partition, callBack, queue, isQueueConsumed)

      //start tread to consume queue and doing callback's on it
      transactionRelay.startConsumeAndCallbackQueueAsync()

      //start tread to consume all transactions before lasttxn including it
      if (lastTransactionOpt.isDefined)
        transactionRelay.consumeTransactionsLessOrEqualThanAsync(lastTransactionOpt.get.getTxnUUID)

      transactionRelay.startListenIncomingTransactions()

      //consume all messages greater than last
      if (lastTransactionOpt.isDefined)
        transactionRelay.consumeTransactionsMoreThan(lastTransactionOpt.get.getTxnUUID)
      else{
        val oldestUuid = options.txnGenerator.getTimeUUID(0)
        transactionRelay.consumeTransactionsMoreThan(oldestUuid)
      }

      transactionRelay.startUpdatingQueueAsync()
    }
  }

  /**
   * Stop consumer handle incoming messages
   */
  def stop() = {
    if (!isStarted)
      throw new IllegalStateException("subscriber not started")

    isQueueConsumed.set(false)
    isStarted = false
  }
}
