package com.bwsw.tstreams.interaction.transactions.transport.traits

import com.bwsw.tstreams.interaction.transactions.messages._

/**
 * Basic trait for transport
 */
trait ITransport {

  /**
   * Request to disable concrete master
   * @param msg Msg to disable master
   * @param timeout Timeout for waiting
   * @return DeleteMasterResponse or null
   */
  def deleteMasterRequest(msg : DeleteMasterRequest, timeout : Int) : IMessage

  /**
   * Request to set concrete master
   * @param msg Message
   * @param timeout Timeout to wait master
   * @return SetMasterResponse or null
   */
  def setMasterRequest(msg : SetMasterRequest, timeout : Int) : IMessage

  /**
   * Request to get Txn
   * @param msg Message
   * @param timeout Timeout to wait master
   * @return TransactionResponse or null
   */
  def transactionRequest(msg: TransactionRequest, timeout: Int): IMessage

  /**
   * Request to figure out state of receiver
   * @param msg Message
   * @return PingResponse or null
   */
  def pingRequest(msg : PingRequest, timeout : Int) : IMessage

  /**
   * Wait incoming requests(every p2p agent must handle this incoming messages)
   * @return IMessage or null
   */
  def waitRequest(): IMessage

  /**
   * Send response to requester
   * @param msg IMessage
   */
  def response(msg : IMessage) : Unit

  /**
   * Send empty request
   * @param msg EmptyRequest
   */
  def stopRequest(msg : EmptyRequest) : Unit

  /**
   * Bind local agent address in transport
   */
  def bindLocalAddress(address : String) : Unit

  /**
   * Stop transport listen incoming messages
   */
  def unbindLocalAddress() : Unit
}

