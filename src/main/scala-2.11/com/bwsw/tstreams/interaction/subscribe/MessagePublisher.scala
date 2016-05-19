package com.bwsw.tstreams.interaction.subscribe

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.interaction.subscribe.client.Broadcaster
import com.bwsw.tstreams.interaction.subscribe.messages.ProducerTopicMessage
import org.apache.zookeeper.{WatchedEvent, Watcher}


class MessagePublisher(prefix : String,
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
        updateSubscribers(p)
        zkService.setWatcher(s"/subscribers/$streamName/$p/event", this)
      }
    }
    zkService.setWatcher(s"/subscribers/$streamName/$p/event", watcher)
  }

  def broadcast(msg : ProducerTopicMessage) = {
    lock.lock()
    broadcaster.broadcast(msg)
    lock.unlock()
  }

  private def updateSubscribers(partition : Int) = {
    lock.lock()
    val subscribersOpt = zkService.getAllSubNodesData[String](s"/subscribers/$streamName/$partition/agents")
    if (subscribersOpt.isDefined){
      broadcaster.updateSubscribers(subscribersOpt.get)
    }
    lock.unlock()
  }
}
