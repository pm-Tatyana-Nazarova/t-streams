package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.ProducerTransactionStatus._
import com.bwsw.tstreams.coordination.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import org.redisson.core.{MessageListener, RTopic}
import scala.util.control.Breaks._


class SubscriberTransactionsRelay[DATATYPE,USERTYPE](subscriber : BasicSubscribingConsumer[DATATYPE,USERTYPE],
                                                     offset: UUID,
                                                     partition : Int,
                                                     callback: BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                     queue : PersistentTransactionQueue,
                                                     isQueueConsumed : AtomicBoolean) {

  /**
   * Constant for Thread.sleep
   */
  private val SLEEPTIME = 5000 // 5 seconds

  /**
   * Serializer to serialize/deserialize incoming messages
   */
  private val serializer = new JsonSerializer

  /**
   * Buffer to maintain all available transactions
   */
  private val transactionBuffer  = new TransactionsBuffer

  /**
   * Fair lock for locking access to transaction buffer
   */
  private val fairLock = new ReentrantLock(true)

  /**
   * Last consumed transaction from map
   */
  private var lastConsumedTransaction : UUID = subscriber.options.txnGenerator.getTimeUUID(0)

  /**
   * Name of the stream
   */
  private val streamName = subscriber.stream.getName

  /**
   * Listener for this relay (!only one must be)
   */
  private var listener : Option[Int] = None

  /**
   * Event topic
   */
  private val topic: RTopic[String] = subscriber.stream.coordinator.getTopic[String](s"$streamName/$partition/events")

  /**
   * Start consume transaction queue async
   */
  def startConsumeAndCallbackQueueAsync() = {
    isQueueConsumed.set(true)
    val latch = new CountDownLatch(1)
    val queueConsumer = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        while (isQueueConsumed.get()) {
          val txn = queue.get()
          callback.onEvent(subscriber, partition, txn)
        }
      }
    })
    queueConsumer.start()
    latch.await()
  }

  /**
   * Consume all transactions in interval (offset;transactionUUID]
   * @param transactionUUID Right border to consume
   */
  def consumeTransactionsLessOrEqualThanAsync(transactionUUID : UUID) = {
    val latch = new CountDownLatch(1)
    val transactionsConsumerBeforeLast = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()

        val transactions = subscriber.stream.metadataStorage.commitEntity.getTransactionsMoreThanAndLessOrEqualThan(
          streamName = streamName,
          partition = partition,
          leftBorder = offset,
          rightBorder = transactionUUID)

        while (transactions.nonEmpty) {
          val uuid = transactions.dequeue().txnUuid
          queue.put(uuid)
        }
      }
    })
    transactionsConsumerBeforeLast.start()
    latch.await()
  }

  /**
   * Consume all transaction starting from transactionUUID without including it
   * @param transactionUUID Left border to consume
   */
  def consumeTransactionsMoreThan(transactionUUID : UUID) = {
    val messagesGreaterThanLast =
        subscriber.stream.metadataStorage.commitEntity.getTransactionsMoreThan(
          streamName,
          partition,
          transactionUUID)

    fairLock.lock()
    messagesGreaterThanLast foreach { m =>
      transactionBuffer.update(m.txnUuid, ProducerTransactionStatus.closed, m.ttl)
    }
    fairLock.unlock()
  }

  /**
   * Starting listen updates from producers about incoming transactions
   * @return Listener ID
   */
  def startListenIncomingTransactions() : Unit = {
    if (listener.isDefined)
      throw new IllegalStateException("listener for this stream/partition already exist")
    
    val listenerId = topic.addListener(new MessageListener[String] {
      override def onMessage(channel: String, rawMsg: String): Unit = {
        fairLock.lock()

        val msg = serializer.deserialize[ProducerTopicMessage](rawMsg)

        if (msg.txnUuid.timestamp() > lastConsumedTransaction.timestamp())
          transactionBuffer.update(msg.txnUuid, msg.status, msg.ttl)

        fairLock.unlock()
      }
    })
    //wait until listener start
    Thread.sleep(SLEEPTIME)

    listener = Some(listenerId)
  }

  /**
   * Remove topic listener
   * @param listenerId Listener ID which can be acquired by [[startListenIncomingTransactions()]]
   */
  def removeListener(listenerId : Int) =
    topic.removeListener(listenerId)


  /**
   * Start pushing data in persistent queue from transaction buffer
   */
  def startUpdatingQueueAsync() : Unit = {
    val latch = new CountDownLatch(1)

    val updateThread =
    new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()

        //start handling map
        while (isQueueConsumed.get()) {
          fairLock.lock()

          val it = transactionBuffer.getIterator()
          breakable {
            while (it.hasNext) {
              val entry = it.next()
              val key: UUID = entry.getKey
              val (status: ProducerTransactionStatus, _) = entry.getValue
              status match {
                case ProducerTransactionStatus.opened =>
                  break()
                case ProducerTransactionStatus.closed =>
                  queue.put(key)
              }

              //TODO remove after complex testing
              if (lastConsumedTransaction.timestamp() >= key.timestamp())
                throw new IllegalStateException("incorrect subscriber state")

              lastConsumedTransaction = key
              it.remove()
            }
          }

          fairLock.unlock()
          Thread.sleep(callback.frequency * 1000L)
        }

        assert(listener.isDefined)
        topic.removeListener(listener.get)
      }
    })
    updateThread.start()
    latch.await()
  }
}
