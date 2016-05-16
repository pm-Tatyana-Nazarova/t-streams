package com.bwsw.tstreams.interaction.transport.impl.client

import java.io.{InputStreamReader, BufferedReader}
import java.net.{ConnectException, Socket, SocketException}
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.interaction.messages.IMessage

/**
 * Client for sending IMessage messages
 */
class TcpIMessageClient {
  private val addressToConnection = scala.collection.mutable.Map[String, Socket]()
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
      val sock = addressToConnection(rcvAddress)
      if (sock.isClosed || !sock.isConnected || sock.isOutputShutdown) {
        addressToConnection.remove(rcvAddress)
        sendAndWaitResponse(msg, timeout)
      }
      else
        writeMsgAndWaitResponse(sock, msg)
    } else {
      try {
        val splits = rcvAddress.split(":")
        assert(splits.size == 2)
        val host = splits(0)
        val port = splits(1).toInt
        val sock = new Socket(host, port)
        sock.setSoTimeout(timeout*1000)
        addressToConnection(rcvAddress) = sock
        writeMsgAndWaitResponse(sock, msg)
      } catch {
        case e: ConnectException => null.asInstanceOf[IMessage]
      }
    }
  }

  /**
   * Helper method for [[sendAndWaitResponse]]]
   * @param socket Socket to send msg
   * @param msg Msg to send
   * @return Response message
   */
  private def writeMsgAndWaitResponse(socket : Socket, msg : IMessage) : IMessage = {
    //do request
    val string = serializer.serialize(msg) + "\n" // "\n" separator is used on server side to separate incoming messages
    val outputStream =
      try {
        socket.getOutputStream
      } catch {
        case e : SocketException =>
          return null.asInstanceOf[IMessage]
      }
    outputStream.write(string.getBytes)
    outputStream.flush()

    //wait response with timeout
    val answer = {
      try {
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
        val string = reader.readLine()
        val response = serializer.deserialize[IMessage](string)
        response
      }
      catch {
        case e : java.net.SocketTimeoutException => null.asInstanceOf[IMessage]
        case e : com.fasterxml.jackson.core.JsonParseException => null.asInstanceOf[IMessage]
        case e : SocketException => null.asInstanceOf[IMessage]
        case e : java.lang.NullPointerException => null.asInstanceOf[IMessage]
      }
    }
    if (answer == null) {
      socket.close()
      addressToConnection.remove(msg.receiverID)
    }

    answer
  }
}