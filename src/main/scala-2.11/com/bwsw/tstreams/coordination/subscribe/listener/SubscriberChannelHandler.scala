package com.bwsw.tstreams.coordination.subscribe.listener

import java.util
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.coordination.subscribe.messages.{ProducerTransactionStatus, ProducerTopicMessage}
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
  private val lockCount = new ReentrantLock(true)
  private val queue = new LinkedBlockingQueue[ProducerTopicMessage]()
  private var callbackThread : Thread = null
  private val isCallback = new AtomicBoolean(true)

  def addCallback(callback : (ProducerTopicMessage)=>Unit) = {
    callbacks += callback
  }

  def startCallBack() = {
    callbackThread = new Thread(new Runnable {
      override def run(): Unit = {
        while(isCallback.get()) {
          val msg = queue.take()
          callbacks.foreach(x => x(msg))
        }
      }
    })
    callbackThread.start()
  }

  def stopCallback() = {
    isCallback.set(false)
    queue.put(ProducerTopicMessage(UUID.randomUUID(),0,ProducerTransactionStatus.cancelled,-1))
    callbackThread.join()
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
    logger.debug(s"[READ PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} ttl=${msg.ttl} status=${msg.status}")
    queue.put(msg)
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
