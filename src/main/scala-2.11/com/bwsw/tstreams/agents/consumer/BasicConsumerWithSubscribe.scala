package com.bwsw.tstreams.agents.consumer

import java.util
import java.util.concurrent.CountDownLatch
import java.util.{Comparator, UUID}
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.{ProducerTransactionStatus, ProducerTopicMessage}
import com.bwsw.tstreams.coordination.ProducerTransactionStatus._
import com.bwsw.tstreams.streams.BasicStream
import java.util.concurrent.atomic.AtomicBoolean
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import com.gilt.timeuuid.TimeUuid
import org.apache.commons.collections4.map.PassiveExpiringMap
import org.redisson.core.{RTopic, MessageListener}
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
class BasicConsumerWithSubscribe[DATATYPE, USERTYPE](name : String,
                                                     stream : BasicStream[DATATYPE],
                                                     options : BasicConsumerOptions[DATATYPE,USERTYPE],
                                                     callBack : BasicConsumerCallback,
                                                     persistentQueuePath : String)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options){

  /**
   * Indicate finished or not subscribe job
   */
  private val finished = new AtomicBoolean(false)

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
      //getting last txn
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

      //start tread to consume all transactions before lasttxn
      if (lastTransactionOpt.isDefined) {
        val txnUuid = lastTransactionOpt.get.getTxnUUID
        val latch = new CountDownLatch(1)
        val transactionsConsumerBeforeLast = new Thread(new Runnable {
          override def run(): Unit = {
            latch.countDown()
            val leftBorder = currentOffsets(partition)
            val transactions = stream.metadataStorage.commitEntity.getTransactionsMoreThanAndLessOrEqualThan(
              stream.getName,
              partition,
              leftBorder,
              txnUuid)
            while (transactions.nonEmpty && !finished.get()) {
              val uuid = transactions.dequeue().time
              queue.put(uuid)
            }
          }
        })
        transactionsConsumerBeforeLast.start()
        latch.await()
      }

      //start tread to consume queue and doing call callback on it
      var latch = new CountDownLatch(1)
      val queueConsumer = new Thread(new Runnable {
        override def run(): Unit = {
          latch.countDown()
          while (!finished.get()) {
            val txn = queue.get()
            callBack.onEvent(partition, txn)
          }
        }
      })
      queueConsumer.start()
      latch.await()

      latch = new CountDownLatch(1)
      val subscriber = new Thread(new Runnable {
        override def run(): Unit = {
          latch.countDown()
          val jsonSerializer = new JsonSerializer
          val lock = new ReentrantLock(true)

          val tree = new util.TreeMap[UUID, (ProducerTransactionStatus, Long /*ttl*/ )](new Comparator[UUID] {
            override def compare(first: UUID, second: UUID): Int = {
              val tsFirst = first.timestamp()
              val tsSecond = second.timestamp()
              if (tsFirst > tsSecond) 1
              else if (tsFirst < tsSecond) -1
              else 0
            }
          })

          val map = new PassiveExpiringMap[UUID, (ProducerTransactionStatus, Long)](
            new PassiveExpiringMap.ExpirationPolicy[UUID, (ProducerTransactionStatus, Long)] {
              override def expirationTime(key: UUID, value: (ProducerTransactionStatus, Long)): Long = {
                if (value._2 == -1) -1
                else System.currentTimeMillis() + value._2 * 1000L
              }
            }, tree)

          val topic: RTopic[String] = stream.coordinator.getTopic[String](s"${stream.getName}/$partition/events")

          val listener = topic.addListener(new MessageListener[String] {
            override def onMessage(channel: String, msg: String): Unit = {
              lock.lock()
              val deserializedMsg = jsonSerializer.deserialize[ProducerTopicMessage](msg)
              if (deserializedMsg.status == ProducerTransactionStatus.canceled)
                map.remove(deserializedMsg.txnUuid)
              else {
                if (!(deserializedMsg.status == ProducerTransactionStatus.closed && map.containsKey(deserializedMsg.txnUuid))) {
                  map.put(deserializedMsg.txnUuid, (deserializedMsg.status, deserializedMsg.ttl))
                }
              }
              lock.unlock()
            }
          })
          //wait listener start
          Thread.sleep(5000)

          //consume all messages greater than last
          val messagesGreaterThanLast =
            if (lastTransactionOpt.isDefined) {
              val lastTransaction = lastTransactionOpt.get
              stream.metadataStorage.commitEntity.getTransactionsMoreThan(
                stream.getName,
                partition,
                lastTransaction.getTxnUUID)
            }
            else {
              stream.metadataStorage.commitEntity.getTransactionsMoreThan(
                stream.getName,
                partition,
                TimeUuid(0))
            }

          lock.lock()
          messagesGreaterThanLast foreach { m =>
            if (m.totalItems != -1)
              map.put(m.time, (ProducerTransactionStatus.closed, -1))
            else
              map.put(m.time, (ProducerTransactionStatus.opened, m.ttl))
          }
          lock.unlock()

          //TODO remove after complex debug
          var valueChecker = TimeUuid(0)

          //start handling map
          while (!finished.get()) {
            lock.lock()

            val it = map.entrySet().iterator()
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
                if (valueChecker.timestamp() >= key.timestamp())
                  throw new IllegalStateException("incorrect subscriber state")

                valueChecker = key

                it.remove()
              }
            }

            lock.unlock()
            Thread.sleep(callBack.frequency * 1000L)
          }
          topic.removeListener(listener)
        }
      })
      subscriber.start()
      latch.await()
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
