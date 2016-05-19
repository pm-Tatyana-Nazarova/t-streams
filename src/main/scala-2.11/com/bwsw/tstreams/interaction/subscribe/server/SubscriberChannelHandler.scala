package com.bwsw.tstreams.interaction.subscribe.server

import java.util

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.interaction.subscribe.messages.ProducerTopicMessage
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.util.ReferenceCountUtil


class SubscriberChannelHandler(callback : (ProducerTopicMessage)=>Unit) extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    callback(msg)
    ReferenceCountUtil.release(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }
}


class ProducerTopicMessageDecoder extends MessageToMessageDecoder[String]{
  val serializer = new JsonSerializer

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[ProducerTopicMessage](msg))
    }
    catch {
      case e : com.fasterxml.jackson.core.JsonParseException =>
      case e : com.fasterxml.jackson.databind.JsonMappingException =>
    }
  }
}
