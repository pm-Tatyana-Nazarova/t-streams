package com.bwsw.tstreams.coordination.subscribe.listener

import java.util
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTopicMessage
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

@Sharable
class SubscriberChannelHandler extends SimpleChannelInboundHandler[ProducerTopicMessage] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var count = 0
  private val callbacks = new ListBuffer[(ProducerTopicMessage)=>Unit]()
  private val lockCallbacks = new ReentrantLock(true)
  private val lockCount = new ReentrantLock(true)

  def addCallback(callback : (ProducerTopicMessage)=>Unit) = {
    lockCallbacks.lock()
    callbacks += callback
    lockCallbacks.unlock()
  }

  def getCount(): Int = {
    lockCount.lock()
    val cnt = count
    lockCount.unlock()
    cnt
  }

  override def channelActive(ctx: ChannelHandlerContext) : Unit = {
    lockCount.lock()
    count += 1
    lockCount.unlock()
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: ProducerTopicMessage): Unit = {
    logger.debug(s"[READ BEFORELOCK PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} ttl=${msg.ttl} status=${msg.status}")
    lockCallbacks.lock()
    logger.debug(s"[READ AFTERLOCK PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} ttl=${msg.ttl} status=${msg.status}")
    callbacks.foreach(c=>c(msg))
    lockCallbacks.unlock()
    ReferenceCountUtil.release(msg)//TODO
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
