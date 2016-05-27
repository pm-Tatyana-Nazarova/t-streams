package com.bwsw.tstreams.coordination.subscribe.publisher

import java.util
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTopicMessage
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
  }

  //triggered on disconnect
  override def channelInactive(ctx: ChannelHandlerContext) : Unit = {
    lock.lock()
    val id = ctx.channel().id()
    val address = idToAddress(id)
    idToAddress.remove(id)
    addressToId.remove(address)
    lock.unlock()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }

  def broadcast(msg : ProducerTopicMessage) = {
    logger.debug(s"[BROADCASTER PUBLISH] partition=${msg.partition} status=${msg.status} uuid=${msg.txnUuid.timestamp()}\n")
    group.writeAndFlush(msg).await()
  }

  def updateSubscribers(newSubscribers : List[String]): Unit = {
    lock.lock()
    logger.debug(s"[BROADCASTER] start updating subscribers:{${addressToId.keys.mkString(",")}}" +
      s" using newSubscribers:{${newSubscribers.mkString(",")}}\n")
    newSubscribers.diff(addressToId.keys.toList) foreach { subscriber =>
      broadcaster.connect(subscriber)
    }
    logger.debug(s"[BROADCASTER] updated subscribers:{${addressToId.keys.mkString(",")}}, current group size: {${group.size()}}\n")
    lock.unlock()
  }

  def updateMap(channelId: ChannelId, address : String) = {
    lock.lock()
    idToAddress(channelId) = address
    addressToId(address) = channelId
    lock.unlock()
  }
}

class ProducerTopicMessageEncoder extends MessageToMessageEncoder[ProducerTopicMessage]{
  val serializer = new JsonSerializer

  override def encode(ctx: ChannelHandlerContext, msg: ProducerTopicMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}