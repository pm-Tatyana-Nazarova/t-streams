package com.bwsw.tstreams.agents.consumer

import java.util
import java.util.concurrent.CountDownLatch
import java.util.{Comparator, UUID}
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.{ProducerMessageSerializer, ProducerTransactionStatus, ProducerTopicMessage}
import com.bwsw.tstreams.coordination.ProducerTransactionStatus._
import com.bwsw.tstreams.streams.BasicStream
import java.util.concurrent.atomic.AtomicBoolean
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
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
//TODO add logging
class BasicSubscribingConsumer[DATATYPE, USERTYPE](name : String,
                                                     stream : BasicStream[DATATYPE],
                                                     options : BasicConsumerOptions[DATATYPE,USERTYPE],
                                                     callBack : BasicConsumerCallback[DATATYPE, USERTYPE],
                                                     persistentQueuePath : String)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options){

  private val link = this

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

            //TODO remove after complex debug
            if (transactions.nonEmpty) {
              assert(transactions.last.time == txnUuid, "last transaction was not added in queue")
            }

            while (transactions.nonEmpty && !finished.get()) {
              val uuid = transactions.dequeue().time
              queue.put(uuid)
            }
          }
        })
        transactionsConsumerBeforeLast.start()
        latch.await()
      }

      //start tread to consume queue and doing callback's on it
      var latch = new CountDownLatch(1)
      val queueConsumer = new Thread(new Runnable {
        override def run(): Unit = {
          latch.countDown()
          while (!finished.get()) {
            val txn = queue.get()
            callBack.onEvent(link, partition, txn)
          }
        }
      })
      queueConsumer.start()
      latch.await()

      latch = new CountDownLatch(1)
      val subscriber = new Thread(new Runnable {
        override def run(): Unit = {
          latch.countDown()
          val lock = new ReentrantLock(true)
          val map = createSortedExpiringMap()

          //UUID to indicate on last handled transaction
          var currentTransactionUUID = options.txnGenerator.getTimeUUID(0)

          val topic: RTopic[String] = stream.coordinator.getTopic[String](s"${stream.getName}/$partition/events")

          val listener = topic.addListener(new MessageListener[String] {
            override def onMessage(channel: String, rawMsg: String): Unit = {
              lock.lock()
              val msg = ProducerMessageSerializer.deserialize(rawMsg)

              if (msg.txnUuid.timestamp() > currentTransactionUUID.timestamp()) {
                if (msg.status == ProducerTransactionStatus.cancelled)
                  map.remove(msg.txnUuid)
                else
                  map.put(msg.txnUuid, (msg.status, msg.ttl))
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
                options.txnGenerator.getTimeUUID(0))
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
          var valueChecker = options.txnGenerator.getTimeUUID(0)

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

                currentTransactionUUID = key
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
   * Create sorted(based on UUID) expiring map
   * @return
   */
  def createSortedExpiringMap(): PassiveExpiringMap[UUID, (ProducerTransactionStatus, Long /*ttl*/ )] = {

    //create tree structure which keeps their keys in sorted order based on keys UUID's
    val treeMap = new util.TreeMap[UUID, (ProducerTransactionStatus, Long /*ttl*/ )](new Comparator[UUID] {
      override def compare(first: UUID, second: UUID): Int = {
        val tsFirst = first.timestamp()
        val tsSecond = second.timestamp()
        if (tsFirst > tsSecond) 1
        else if (tsFirst < tsSecond) -1
        else 0
      }
    })

    //create map with expiring records
    val map = new PassiveExpiringMap[UUID, (ProducerTransactionStatus, Long /*ttl*/ )](
      new PassiveExpiringMap.ExpirationPolicy[UUID, (ProducerTransactionStatus, Long /*ttl*/ )] {
        override def expirationTime(key: UUID, value: (ProducerTransactionStatus, Long /*ttl*/ )): Long = {
          if (value._2 < 0)
            -1 //just need to keep records without ttl
          else
            System.currentTimeMillis() + value._2 * 1000L
        }
      }, treeMap)
    map
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
