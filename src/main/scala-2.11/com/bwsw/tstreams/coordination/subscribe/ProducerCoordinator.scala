package com.bwsw.tstreams.coordination.subscribe

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.coordination.subscribe.publisher.Broadcaster
import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTopicMessage
import org.apache.zookeeper.{WatchedEvent, Watcher}


class ProducerCoordinator(prefix : String,
                          streamName : String,
                          usedPartitions : List[Int],
                          zkHosts : List[InetSocketAddress],
                          zkSessionTimeout : Int) {
  private val zkService = new ZkService(prefix, zkHosts, zkSessionTimeout)
  private val broadcaster = new Broadcaster
  private val lock = new ReentrantLock(true)

  usedPartitions foreach { p =>
    val watcher = new Watcher {
      override def process(event: WatchedEvent): Unit = {
        println("event")
        updateSubscribers(p)
        zkService.setWatcher(s"/subscribers/event/$streamName/$p", this)
      }
    }
    zkService.setWatcher(s"/subscribers/event/$streamName/$p", watcher)
    updateSubscribers(p)
  }

  def publish(msg : ProducerTopicMessage) = {
    lock.lock()
    broadcaster.broadcast(msg)
    lock.unlock()
  }

  private def updateSubscribers(partition : Int) = {
    lock.lock()
    val subscribersPathOpt = zkService.getAllSubNodesData[String](s"/subscribers/agents/$streamName/$partition")
    if (subscribersPathOpt.isDefined)
      broadcaster.updateSubscribers(subscribersPathOpt.get)
    lock.unlock()
  }
}
