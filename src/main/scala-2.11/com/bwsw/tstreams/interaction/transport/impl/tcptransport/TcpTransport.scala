package com.bwsw.tstreams.interaction.transport.impl.tcptransport

import java.util.concurrent.LinkedBlockingQueue

import com.bwsw.tstreams.interaction.messages._
import com.bwsw.tstreams.interaction.transport.traits.ITransport

class TcpTransport(msgHandleInterval : Int) extends ITransport{
  private var listener : TcpIMessageServer = null
  private val sender : TcpIMessageClient = new TcpIMessageClient
  private val msgQueue = new LinkedBlockingQueue[IMessage]()

  /**
   * Request to disable concrete master
   * @param msg Msg to disable master
   * @param timeout Timeout for waiting
   * @return DeleteMasterResponse or null
   */
  override def deleteMasterRequest(msg: DeleteMasterRequest, timeout: Int): IMessage = {
    val response = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Request to figure out state of receiver
   * @param msg Message
   * @return PingResponse or null
   */
  override def pingRequest(msg: PingRequest, timeout: Int): IMessage = {
    val response = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Wait incoming requests(every p2p agent must handle this incoming messages)
   * @return IMessage or null
   */
  override def waitRequest(): IMessage =
    msgQueue.take()

  /**
   * Send empty request (just for testing)
   * @param msg EmptyRequest
   */
  override def stopRequest(msg: EmptyRequest): Unit = {
    sender.sendAndWaitResponse(msg, 3)
  }

  /**
   * Request to set concrete master
   * @param msg Message
   * @param timeout Timeout to wait master
   * @return SetMasterResponse or null
   */
  override def setMasterRequest(msg: SetMasterRequest, timeout: Int): IMessage = {
    val response: IMessage = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Request to get Txn
   * @param msg Message
   * @param timeout Timeout to wait master
   * @return TransactionResponse or null
   */
  override def transactionRequest(msg: TransactionRequest, timeout: Int): IMessage = {
    val response: IMessage = sender.sendAndWaitResponse(msg, timeout)
    response
  }

  /**
   * Send response to requester
   * @param msg IMessage
   */
  override def response(msg: IMessage): Unit = {
    listener.response(msg)
  }

  /**
   * Bind local agent address in transport
   */
  override def bindLocalAddress(address: String): Unit = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val port = splits(1).toInt
    listener = new TcpIMessageServer(port = port,
    newMessageCallback = (msg: IMessage) => {
      msgQueue.add(msg)
    },
    msgHandleInterval = msgHandleInterval)
    listener.startServer()
  }

  //for testing purposes
  override def unbindLocalAddress(): Unit = {
    listener.stop()
  }
}