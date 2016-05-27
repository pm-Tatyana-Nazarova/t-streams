package com.bwsw.tstreams.coordination.transactions.transport.impl.server

import java.util
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.transactions.messages.IMessage
import io.netty.channel._
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import io.netty.util.ReferenceCountUtil

import scala.collection.mutable.ListBuffer

@ChannelHandler.Sharable
class IMessageServerChannelHandler extends SimpleChannelInboundHandler[IMessage] {
  private val lock = new ReentrantLock(true)
  private val idToChannel = scala.collection.mutable.Map[ChannelId, Channel]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val callbacks = ListBuffer[(IMessage) => Unit]()

  def addCallback(callback : (IMessage) => Unit) = {
    lock.lock()
    callbacks += callback
    lock.unlock()
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: IMessage): Unit = {
    lock.lock()
    val address = msg.senderID
    val id = ctx.channel().id()
    val channel = ctx.channel()
    if (!idToChannel.contains(id)){
      idToChannel(id) = channel
      addressToId(address) = id
      idToAddress(id) = address
    }
    callbacks.foreach(x=>x(msg))
    ReferenceCountUtil.release(msg)
    lock.unlock()
  }

  //triggered on disconnect
  override def channelInactive(ctx: ChannelHandlerContext) : Unit = {
    lock.lock()
    val id = ctx.channel().id()
    if (idToChannel.contains(id)) {
      idToChannel.remove(id)
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
      val channel = idToChannel(id)
      channel.writeAndFlush(msg).await()
    }
    lock.unlock()
  }
}


class IMessageDecoder extends MessageToMessageDecoder[String]{
  val serializer = new JsonSerializer

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      if (msg != null)
        out.add(serializer.deserialize[IMessage](msg))
    }
    catch {
      case e : com.fasterxml.jackson.core.JsonParseException =>
      case e : com.fasterxml.jackson.databind.JsonMappingException =>
    }
  }
}


class IMessageEncoder extends MessageToMessageEncoder[IMessage]{
  val serializer = new JsonSerializer

  override def encode(ctx: ChannelHandlerContext, msg: IMessage, out: util.List[AnyRef]): Unit = {
    out.add(serializer.serialize(msg) + "\n")
  }
}