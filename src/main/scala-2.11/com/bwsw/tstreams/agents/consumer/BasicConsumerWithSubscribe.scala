package com.bwsw.tstreams.agents.consumer

import java.util
import java.util.{Comparator, UUID}
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.{ProducerTransactionStatus, ProducerTopicMessage}
import com.bwsw.tstreams.coordination.ProducerTransactionStatus._
import com.bwsw.tstreams.streams.BasicStream
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.commons.collections4.map.PassiveExpiringMap
import org.redisson.core.{RTopic, MessageListener}
import scala.util.control.Breaks._

/**
 * Basic consumer with subscribe option
 * @param name Name of consumer
 * @param stream Stream from which to consume transactions
 * @param options Basic consumer options
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
class BasicConsumerWithSubscribe[DATATYPE, USERTYPE](name : String,
                                                     stream : BasicStream[DATATYPE],
                                                     options : BasicConsumerOptions[DATATYPE,USERTYPE],
                                                     callBack : BasicConsumerCallback)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options){

  /**
   * Indicate finished or not subscribe job
   */
  private val finishedSubscribe = new AtomicBoolean(false)

  val Threads =
  for (partition <- 0 until stream.getPartitions) yield
    new Thread(new Runnable {
      override def run(): Unit = {
        val jsonSerializer = new JsonSerializer
        val lock = new ReentrantLock()

        val tree = new util.TreeMap[UUID,(ProducerTransactionStatus,Long/*ttl*/)](new Comparator[UUID] {
          override def compare(first: UUID, second: UUID): Int = {
            val tsFirst = first.timestamp()
            val tsSecond = second.timestamp()
            if (tsFirst > tsSecond) 1
            else if (tsFirst < tsSecond) -1
            else 0
          }
        })

        val map = new PassiveExpiringMap[UUID,(ProducerTransactionStatus,Long)](
          new PassiveExpiringMap.ExpirationPolicy[UUID,(ProducerTransactionStatus,Long)] {
            override def expirationTime(key: UUID, value: (ProducerTransactionStatus,Long)): Long = {
              if (value._2 == -1) -1
              else System.currentTimeMillis() + value._2*1000L
            }
          }, tree)

        val topic: RTopic[String] = stream.coordinator.getTopic[String](s"${stream.getName}/$partition/events")

        val listener = topic.addListener(new MessageListener[String] {
          override def onMessage(channel: String, msg: String): Unit = {
            lock.lock()

            val deserializedMsg = jsonSerializer.deserialize[ProducerTopicMessage](msg)
            if (deserializedMsg.status == ProducerTransactionStatus.canceled) {
              if (map.containsKey(deserializedMsg.txnUuid)) {
                map.remove(deserializedMsg.txnUuid)
              }
            }
            else {
                map.put(deserializedMsg.txnUuid, (deserializedMsg.status, deserializedMsg.ttl))
            }

            lock.unlock()
          }
        })

        while(!finishedSubscribe.get()){

          lock.lock()
          val it = map.entrySet().iterator()

          breakable{while (it.hasNext) {
            val entry = it.next()
            val key: UUID = entry.getKey
            val (status: ProducerTransactionStatus, _) = entry.getValue
            status match {
              case ProducerTransactionStatus.opened =>
                break()
              case ProducerTransactionStatus.closed =>
                callBack.onEvent(partition, key)
            }
            it.remove()
          }}
          lock.unlock()

          Thread.sleep(callBack.frequency * 1000L)
        }

        topic.removeListener(listener)
      }
    })

  /**
   * Stop consumer handle incoming messages
   */
  def stopListen() = finishedSubscribe.set(true)

  /**
   * Start consumer handle incoming messages
   */
  def startListen() = Threads.foreach(t=>t.start())
}
