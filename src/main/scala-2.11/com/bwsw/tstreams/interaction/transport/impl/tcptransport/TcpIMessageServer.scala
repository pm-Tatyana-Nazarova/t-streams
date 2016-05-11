package com.bwsw.tstreams.interaction.transport.impl.tcptransport

import java.io.{OutputStreamWriter, PrintWriter, BufferedReader, InputStreamReader}
import java.net.{SocketException, Socket, ServerSocket}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.interaction.messages.IMessage

/**
 * Server for awaiting for new IMessages
 * @param port Server port
 * @param newMessageCallback Callback on every new message
 * @param msgHandleInterval Interval in milliseconds for messages handling
 */
class TcpIMessageServer(port : Int, newMessageCallback : IMessage => Unit, msgHandleInterval : Int) {
  private val serverSocket = new ServerSocket(port)
  private var listenerThread : Thread = null
  private var msgHandler : Thread = null
  private val isListen: AtomicBoolean = new AtomicBoolean(false)
  private var clientSockets : scala.collection.mutable.Set[(Socket,BufferedReader)] = scala.collection.mutable.Set[(Socket,BufferedReader)]()
  private val addressToConnection = scala.collection.mutable.Map[String, (Socket,PrintWriter)]()
  private val lockAddressToConnection = new ReentrantLock(true)
  private val lockClientSockets = new ReentrantLock(true)
  private val serializer = new JsonSerializer

  /**
   * Starting message handler to callback on every new messages
   */
  private def startMsgHandler() : Unit = {
    val syncPoint = new CountDownLatch(1)
    msgHandler = new Thread(new Runnable {
      override def run(): Unit = {
        syncPoint.countDown()
        while (isListen.get()){
          lockClientSockets.lock()
          clientSockets.retain{case(socket,_) => socket.isConnected && !socket.isInputShutdown && !socket.isClosed}
          clientSockets.foreach{case(sock,bufferedReader) =>
            try {
              while (bufferedReader.ready()) {
                val string = bufferedReader.readLine()
                val msg = serializer.deserialize[IMessage](string)

                lockAddressToConnection.lock()
                addressToConnection.retain{case(_,(socket,_)) => socket.isConnected && !socket.isInputShutdown && !socket.isClosed}
                if (!addressToConnection.contains(msg.senderID))
                  addressToConnection(msg.senderID) = (sock, new PrintWriter(new OutputStreamWriter(sock.getOutputStream)))
                lockAddressToConnection.unlock()

                newMessageCallback(msg)
              }
            }
            catch {
              case e : SocketException => //do nothing here, this socket will be removed on next iteration
              case e : com.fasterxml.jackson.core.JsonParseException =>
                //debug only
                println(e.getMessage)
            }
          }
          lockClientSockets.unlock()
          Thread.sleep(msgHandleInterval)
        }
      }
    })
    msgHandler.start()
    syncPoint.await()
  }

  /**
   * Starting new connection listener for accepting them
   */
  def startListener() : Unit = {
    isListen.set(true)
    val syncPoint = new CountDownLatch(1)
    listenerThread = new Thread(new Runnable {
      override def run() : Unit = {
        syncPoint.countDown()
        while (isListen.get()) {
          val sock = serverSocket.accept()
          lockClientSockets.lock()
          clientSockets += ((sock, new BufferedReader(new InputStreamReader(sock.getInputStream))))
          lockClientSockets.unlock()
        }
      }
    })
    listenerThread.start()
    syncPoint.await()
  }

  /**
   * Doing response with IMessage to agent which made a request
   * @param msg Response msg
   */
  def response(msg : IMessage) : Unit = {
    lockAddressToConnection.lock()
    addressToConnection.retain{case(_,(socket,_)) => socket.isConnected && !socket.isInputShutdown && !socket.isClosed}
    val condition = addressToConnection.contains(msg.receiverID)
    val (_,writer) = if (condition) addressToConnection(msg.receiverID) else (null,null)
    lockAddressToConnection.unlock()

    if (condition) {
      val string = serializer.serialize(msg)
      writer.println(string)
      writer.flush()
    }
  }

  /**
   * Starting Server
   */
  def startServer() : Unit = {
    startListener()
    startMsgHandler()
  }

  /**
   * Stop Server method(debug purposes)
   */
  def stop() : Unit = {
    isListen.set(false)
    msgHandler.join()
    val socket = new Socket("localhost",port).getOutputStream //just to skip socket.accept blocking
    socket.close()
    listenerThread.join()
  }
}