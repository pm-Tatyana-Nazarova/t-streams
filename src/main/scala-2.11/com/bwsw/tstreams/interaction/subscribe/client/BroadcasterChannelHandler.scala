package com.bwsw.tstreams.interaction.subscribe.client

import java.util

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.interaction.subscribe.messages.{ProducerTransactionStatus, ProducerTopicMessage}
import com.datastax.driver.core.utils.UUIDs
import io.netty.channel._
import io.netty.channel.group.DefaultChannelGroup
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.util.concurrent.GlobalEventExecutor

@ChannelHandler.Sharable
class BroadcasterChannelHandler extends SimpleChannelInboundHandler[ProducerTopicMessage] {

  private val group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

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

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }

  def broadcast(msg : ProducerTopicMessage) = {
    group.writeAndFlush(msg)
  }

  def updateSubscribers(newSubscribers : List[String]) = {
    ???
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