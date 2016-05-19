package com.bwsw.tstreams.newcoordination.subscribe

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.newcoordination.subscribe.messages.{ProducerTransactionStatus, ProducerTopicMessage}
import com.bwsw.tstreams.newcoordination.subscribe.listener.ProducerTopicMessageListener
import com.datastax.driver.core.utils.UUIDs
import org.apache.zookeeper.CreateMode


class SubscriberCoordinator(agentAddress : String,
                            prefix : String,
                            zkHosts : List[InetSocketAddress],
                            zkSessionTimeout : Int,
                            onMessageCallback : (ProducerTopicMessage) => Unit) {
  private val SYNCHRONIZE_TIME = 60

  private val zkService = new ZkService(prefix, zkHosts, zkSessionTimeout)
  private val (_,port) = getHostPort(agentAddress)
  private val listener = new ProducerTopicMessageListener(port, onMessageCallback)

  private def getHostPort(address : String): (String, Int) = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    (host, port)
  }

  def startListen() =
    listener.start()

  def registerSubscriber(streamName : String, partition : Int) = {
    zkService.create(s"/subscribers/agents/$streamName/$partition/subscriber_", agentAddress, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  def notifyProducers(streamName : String, partition : Int) = {
    zkService.notify(s"/subscribers/event/$streamName/$partition")
  }

  def synchronize(streamName : String, partition : Int) = {
    listener.resetConnectionsAmount
    val agentsOpt = zkService.getAllSubPath(s"/producers/agents/$streamName/$partition")
    val totalAmount = if (agentsOpt.isEmpty) 0 else agentsOpt.get.size
    var timer = 0
    while (listener.getConnectionsAmount < totalAmount && timer < SYNCHRONIZE_TIME){
      timer += 1
      Thread.sleep(1000)
    }
  }
}