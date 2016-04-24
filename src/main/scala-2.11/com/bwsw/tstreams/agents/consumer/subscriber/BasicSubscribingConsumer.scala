package com.bwsw.tstreams.agents.consumer.subscriber

import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.{Comparator, UUID}

import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.ProducerTransactionStatus._
import com.bwsw.tstreams.coordination.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import org.apache.commons.collections4.map.PassiveExpiringMap
import org.redisson.core.{MessageListener, RTopic}

import scala.util.control.Breaks._

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

  private val link = this

  /**
   * Indicate finished or not subscribe job
   */
  private val finished = new AtomicBoolean(false)

  /**
   *
   */
  private val serializer = new JsonSerializer

  /**
   * Current subscriber state
   */
  private var isStarted = false

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

      val transactionRelay = new SubscriberTransactionsRelay(this, currentOffsets(partition), partition, callBack, queue)

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
  def close() = {
    if (!isStarted)
      throw new IllegalStateException("subscriber not started")

    isStarted = false
    finished.set(true)
  }
}
