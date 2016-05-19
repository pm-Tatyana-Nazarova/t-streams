package com.bwsw.tstreams.coordination.subscribe.publisher

import java.net.InetSocketAddress

import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTopicMessage
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}


class Broadcaster {
  private val group = new NioEventLoopGroup()
  private var bootstrap : Bootstrap = null
  private val channelHandler = new BroadcasterChannelHandler(this)

  bootstrap = new Bootstrap()
  bootstrap
    .group(group)
    .channel(classOf[NioSocketChannel])
    .handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel) {
        val p = ch.pipeline()
        p.addLast("decoder", new StringDecoder())
        p.addLast("encoder", new StringEncoder())
        p.addLast("serializer", new ProducerTopicMessageEncoder())
        p.addLast("handler", channelHandler)
      }
    })

  def connect(address : String) = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    val channelFuture = bootstrap.connect(new InetSocketAddress(host,port)).await()
    if (channelFuture.isSuccess){
      channelHandler.updateMap(channelFuture.channel().id(), address)
    }
  }

  def broadcast(msg : ProducerTopicMessage) = {
    channelHandler.broadcast(msg)
  }

  //TODO mb unsafe
  def close() = {
    group.shutdownGracefully().sync()
  }

  def updateSubscribers(newSubscribers : List[String]) = {
    channelHandler.updateSubscribers(newSubscribers)
  }
}