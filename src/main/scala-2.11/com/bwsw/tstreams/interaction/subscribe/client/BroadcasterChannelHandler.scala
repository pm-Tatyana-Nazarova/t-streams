package com.bwsw.tstreams.interaction.subscribe.client

import java.util
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.interaction.subscribe.messages.{ProducerTransactionStatus, ProducerTopicMessage}
import com.datastax.driver.core.utils.UUIDs
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.util.concurrent.GlobalEventExecutor
import org.slf4j.LoggerFactory

@ChannelHandler.Sharable
class BroadcasterChannelHandler(broadcaster : Broadcaster) extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val lock = new ReentrantLock(true)

  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    throw new IllegalStateException("Broadcaster must only broadcast messages without any response")
  }

  //triggered on connect
  override def channelActive(ctx: ChannelHandlerContext) : Unit = {
    group.add(ctx.channel())
    //send init msg to indicate that channel is active
    ctx.channel().writeAndFlush(ProducerTopicMessage(UUIDs.random(),0,ProducerTransactionStatus.init))
  }

  //triggered on disconnect
  override def channelInactive(ctx: ChannelHandlerContext) : Unit = {
    lock.lock()
    val id = ctx.channel().id()
    assert(idToAddress.contains(id))
    val address = idToAddress(id)
    assert(addressToId.contains(address))
    idToAddress.remove(id)
    addressToId.remove(address)
    lock.unlock()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }

  def broadcast(msg : ProducerTopicMessage) = {
    group.writeAndFlush(msg)
  }

  def updateSubscribers(newSubscribers : List[String]) = {
    lock.lock()
    logger.debug(s"[BROADCASTER] start updating subscribers:{${addressToId.keys.mkString(",")}}" +
      s" using newSubscribers:{${newSubscribers.mkString(",")}}")
    newSubscribers.diff(addressToId.keys.toList) foreach { subscriber =>
      broadcaster.connect(subscriber)
    }
    logger.debug(s"[BROADCASTER] updated subscribers:{${addressToId.keys.mkString(",")}}, current group size: {${group.size()}}")
    assert(group.size() <= addressToId.keys.size)
    lock.unlock()
  }

  def updateMap(channelId: ChannelId, address : String) = {
    idToAddress(channelId) = address
    addressToId(address) = channelId
  }
  
  def closeChannels() = {
    group.close().await()
  }
}

class ProducerTopicMessageEncoder extends MessageToMessageEncoder[ProducerTopicMessage]{
  val serializer = new JsonSerializer

  override def encode(ctx: ChannelHandlerContext, msg: ProducerTopicMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}