package com.bwsw.tstreams.interaction.transport.impl.tcptransport

import java.io.{InputStreamReader, BufferedReader, PrintWriter, OutputStreamWriter}
import java.net.{ConnectException, SocketException, Socket}
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.interaction.messages.IMessage

/**
 * Client for sending IMessage messages
 */
class TcpIMessageClient {
  private val addressToConnection = scala.collection.mutable.Map[String, (Socket,BufferedReader,PrintWriter)]()
  private val serializer = new JsonSerializer

  /**
   * Method for send IMessage and wait response from rcv agent
   * @param msg Message to send
   * @param timeout Timeout for waiting response(null will be returned in case of timeout)
   * @return Response message
   */
  def sendAndWaitResponse(msg : IMessage, timeout : Int) : IMessage = {
    val rcvAddress = msg.receiverID
    if (addressToConnection.contains(rcvAddress)){
      val (sock,reader,writer) = addressToConnection(rcvAddress)
      if (sock.isClosed || !sock.isConnected || sock.isOutputShutdown) {
        addressToConnection.remove(rcvAddress)
        sendAndWaitResponse(msg, timeout)
      }
      else
        writeMsgAndWaitResponse((sock,reader,writer), msg, timeout)
    } else {
      try {
        val splits = rcvAddress.split(":")
        assert(splits.size == 2)
        val host = splits(0)
        val port = splits(1).toInt
        val sock = new Socket(host, port)
        val reader = new BufferedReader(new InputStreamReader(sock.getInputStream))
        val writer = new PrintWriter(new OutputStreamWriter(sock.getOutputStream))
        addressToConnection(rcvAddress) = (sock, reader, writer)
        writeMsgAndWaitResponse((sock,reader,writer), msg, timeout)
      }
      catch {
        case e: ConnectException => null.asInstanceOf[IMessage]
      }
    }
  }

  /**
   * Helper method for [[sendAndWaitResponse]]]
   * @param tuple triplet of Socket its output writer and input reader
   * @param msg Msg to send
   * @param timeout Timeout for waiting response(null will be returned in case of timeout)
   * @return Response message
   */
  private def writeMsgAndWaitResponse(tuple : (Socket,BufferedReader,PrintWriter), msg : IMessage, timeout : Int) : IMessage = {
    val (socket,reader,writer) = tuple
    socket.setSoTimeout(timeout*1000)
    //do request
    val string = serializer.serialize(msg)
    writer.println(string)
    writer.flush()
    //wait response
    val answer = {
      try {
        val string = reader.readLine()
        val response = serializer.deserialize[IMessage](string)
        response
      }
      catch {
        case e: java.net.SocketTimeoutException => null.asInstanceOf[IMessage]
        case e: SocketException => null.asInstanceOf[IMessage]
      }
    }
    if (answer == null) {
      socket.close()
      addressToConnection.remove(msg.receiverID)
    }

    answer
  }
}