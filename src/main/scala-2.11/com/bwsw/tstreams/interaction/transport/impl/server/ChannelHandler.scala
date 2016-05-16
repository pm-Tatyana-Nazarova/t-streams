package com.bwsw.tstreams.interaction.transport.impl.server

import java.util
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.interaction.messages.IMessage
import io.netty.channel._
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}

@ChannelHandler.Sharable
class ChannelHandler(callback : (IMessage) => Unit) extends SimpleChannelInboundHandler[IMessage] {
  private val lock = new ReentrantLock(true)
  private val idToChannel = scala.collection.mutable.Map[ChannelId, Channel]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()

  override def channelRead0(ctx: ChannelHandlerContext, msg: IMessage): Unit = {
    lock.lock()
    val address = msg.senderID
    val id = ctx.channel().id()
    val channel = ctx.channel()
    if (!idToChannel.contains(id)){
      assert(!addressToId.contains(address))
      assert(!idToAddress.contains(id))
      idToChannel(id) = channel
      addressToId(address) = id
      idToAddress(id) = address
    }
    callback(msg)
    lock.unlock()
  }

  //triggered on disconnect
  override def channelInactive(ctx: ChannelHandlerContext) : Unit = {
    lock.lock()
    val id = ctx.channel().id()
    if (idToChannel.contains(id)) {
      idToChannel.remove(id)
      assert(idToAddress.contains(id))
      val address = idToAddress(id)
      idToAddress.remove(id)
      addressToId.remove(address)
    }
    lock.unlock()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause.printStackTrace()
    ctx.close()
  }

  def response(msg : IMessage) : Unit = {
    lock.lock()
    val responseAddress = msg.receiverID
    if (addressToId.contains(responseAddress)){
      val id = addressToId(responseAddress)
      assert(idToChannel.contains(id))
      val channel = idToChannel(id)
      channel.writeAndFlush(msg)
    }
    lock.unlock()
  }
}


class IMessageDecoder extends MessageToMessageDecoder[String]{
  val serializer = new JsonSerializer

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      out.add(serializer.deserialize[IMessage](msg))
    }
    catch {
      case e : com.fasterxml.jackson.core.JsonParseException =>
      case e : com.fasterxml.jackson.databind.JsonMappingException =>
      case e : java.lang.NullPointerException =>
    }
  }
}


class IMessageEncoder extends MessageToMessageEncoder[IMessage]{
  val serializer = new JsonSerializer

  override def encode(ctx: ChannelHandlerContext, msg: IMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}