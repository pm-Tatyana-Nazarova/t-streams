package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.ProducerTransactionStatus.ProducerTransactionStatus
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import org.redisson.core.{MessageListener, RTopic}


class SubscriberTransactionsRelay[DATATYPE,USERTYPE](subscriber : BasicSubscribingConsumer[DATATYPE,USERTYPE],
                                                     offset: UUID,
                                                     partition : Int,
                                                     callback: BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                     queue : PersistentTransactionQueue) {

  /**
   *
   */
  private val SLEEPTIME = 5000 // 5seconds

  /**
   *
   */
  private val serializer = new JsonSerializer

  /**
   * 
   */
  private val map : SortedExpiringMap[UUID, (ProducerTransactionStatus, Long)] =
    new SortedExpiringMap(new UUIDComparator, new SubscriberExpirationPolicy)

  /**
   *
   */
  private val fairLock = new ReentrantLock(true)

  /**
   * 
   */
  private var lastConsumedTransaction : UUID = subscriber.options.txnGenerator.getTimeUUID(0)

  /**
   *
   */
  private val isQueueConsumed = new AtomicBoolean(false)

  /**
   *
   */
  private val streamName = subscriber.stream.getName

  /**
   *
   */
  private val topic: RTopic[String] = subscriber.stream.coordinator.getTopic[String](s"$streamName/$partition/events")

  /**
   * Start consume transaction queue async
   */
  def startConsumeQueueAsync() = {
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
   * Stop consume queue
   */
  def stopConsumeQueue() =
    isQueueConsumed.set(false)


  /**
   * Consume all transactions starting from offset(partition) before transactionUUID(
   * @param transactionUUID Right border to consume
   */
  def consumeAllTransactionsBeforeAsync(transactionUUID : UUID) = {
    val latch = new CountDownLatch(1)
    val transactionsConsumerBeforeLast = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()

        val transactions = subscriber.stream.metadataStorage.commitEntity.getTransactionsMoreThanAndLessOrEqualThan(
          streamName,
          partition,
          offset,
          transactionUUID)

        //TODO remove after complex debug
        if (transactions.nonEmpty) {
          assert(transactions.last.time == transactionUUID, "last transaction was not added in queue")
        }

        while (transactions.nonEmpty) {
          val uuid = transactions.dequeue().time
          queue.put(uuid)
        }
      }
    })
    transactionsConsumerBeforeLast.start()
    latch.await()
  }

  /**
   * Subscriber start
   * @return Listener ID
   */
  def startListenIncomingTransactions() : Int = {
    val listenerId = topic.addListener(new MessageListener[String] {
      override def onMessage(channel: String, rawMsg: String): Unit = {
        fairLock.lock()
        val msg = serializer.deserialize[ProducerTopicMessage](rawMsg)
        if (msg.txnUuid.timestamp() > lastConsumedTransaction.timestamp()) {
          if (msg.status == ProducerTransactionStatus.cancelled)
            map.remove(msg.txnUuid)
          else
            map.put(msg.txnUuid, (msg.status, msg.ttl))
        }
        fairLock.unlock()
      }
    })
    //wait listener start
    Thread.sleep(SLEEPTIME)

    listenerId
  }

  def startListenIncomingTransactions(listenerId : Int) = {

  }
}
