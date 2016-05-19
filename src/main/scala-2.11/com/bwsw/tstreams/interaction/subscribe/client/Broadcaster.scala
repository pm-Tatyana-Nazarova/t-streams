package com.bwsw.tstreams.interaction.subscribe.client

import java.net.InetSocketAddress

import com.bwsw.tstreams.interaction.subscribe.messages.ProducerTopicMessage
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}


class Broadcaster {
  private val group = new NioEventLoopGroup()
  private var bootstrap : Bootstrap = null
  private val channelHandler = new BroadcasterChannelHandler

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

  def connect(address : InetSocketAddress) = {
    bootstrap.connect(address).await()
  }

  def broadcast(msg : ProducerTopicMessage) = {
    channelHandler.broadcast(msg)
  }

  def close() = {
    channelHandler.closeChannels()
    group.shutdownGracefully().sync()
  }

  def updateSubscribers(newSubscribers : List[String]) = {
    channelHandler.updateSubscribers(newSubscribers)
  }
}