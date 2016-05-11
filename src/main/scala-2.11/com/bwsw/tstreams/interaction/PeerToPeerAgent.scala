package com.bwsw.tstreams.interaction

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.producer.BasicProducer
import com.bwsw.tstreams.interaction.messages._
import com.bwsw.tstreams.interaction.transport.traits.ITransport
import com.bwsw.tstreams.interaction.zkservice.{AgentSettings, ZkService}
import org.apache.zookeeper.CreateMode

/**
 * Agent for providing peer to peer interaction between producers
 * @param agentAddress Concrete agent address
 * @param zkHosts ZkHosts to connect
 * @param zkRootPath Common prefix for all zk created entities
 * @param zkTimeout Timeout to connect to zk
 * @param producer Producer reference
 * @param usedPartitions List of used producer partitions
 * @param isLowPriorityToBeMaster Flag which indicate to have low priority to be master
 * @param transport Transport to provide interaction
 * @param transportTimeout Timeout for waiting response
 */
class PeerToPeerAgent(agentAddress : String,
                      zkHosts : List[InetSocketAddress],
                      zkRootPath : String,
                      zkTimeout : Int,
                      producer : BasicProducer[_,_],
                      usedPartitions : List[Int],
                      isLowPriorityToBeMaster : Boolean,
                      transport: ITransport,
                      transportTimeout : Int) {

  protected var zkRetriesAmount = 34
  protected val zkService = new ZkService(zkRootPath, zkHosts, zkTimeout)
  protected val localMasters = scala.collection.mutable.Map[Int/*partition*/, String/*master*/]()
  protected val lockLocalMasters = new ReentrantLock(true)
  protected val lockManagingMaster = new ReentrantLock(true)
  protected val streamName = producer.stream.getName
  init()

  /**
   * Agent initialization
   */
  protected def init() = {
    transport.bindLocalAddress(agentAddress)
    startValidator()
    startHandleMessages()
    usedPartitions foreach {p =>
      val penalty = if (isLowPriorityToBeMaster) 1000*1000 else 0
      zkService.create[AgentSettings](s"/producers/agents/$streamName/$p/unique_agent_$agentAddress" + "_",
       AgentSettings(agentAddress, priority = 0, penalty),
       CreateMode.EPHEMERAL_SEQUENTIAL)
    }
    usedPartitions foreach { p =>
      updateMaster(p, init = true)
    }
  }

  /**
   * Amend agent priority
   * @param partition Partition to update priority
   * @param value Value which will be added to current priority
   */
  protected def updateThisAgentPriority(partition : Int, value : Int) = {
    val agentsOpt = zkService.getAllSubPath(s"/producers/agents/$streamName/$partition")
    assert(agentsOpt.isDefined)
    val agents: List[String] = agentsOpt.get
    val filtered = agents.filter(_ contains s"_${agentAddress}_")
    assert(filtered.size == 1)
    val thisAgentPath = filtered.head
    val agentSettingsOpt = zkService.get[AgentSettings](s"/producers/agents/$streamName/$partition/" + thisAgentPath)
    assert(agentSettingsOpt.isDefined)
    val updatedAgentSettings = agentSettingsOpt.get
    updatedAgentSettings.priority += value
    zkService.setData(s"/producers/agents/$streamName/$partition/" + thisAgentPath, updatedAgentSettings)
  }

  /**
   * Helper method for new master voting
   * @param partition New master partition
   * @param retries Retries to try to set new master
   * @return Selected master address
   */
  protected def startVotingInternal(partition : Int, retries : Int = zkRetriesAmount) : String = {
    val masterID = getMaster(partition)
    val newMaster : String = {
      if (masterID.isDefined)
        masterID.get
      else {
        val agentsOpt = zkService.getAllSubNodesData[AgentSettings](s"/producers/agents/$streamName/$partition")
        assert(agentsOpt.isDefined)
        val agents = agentsOpt.get.sortBy(x=>x.priority-x.penalty)
        val bestMaster = agents.last.id
        transport.setMasterRequest(SetMasterRequest(agentAddress, bestMaster, partition), transportTimeout) match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            startVotingInternal(partition, retries - 1)

          case SetMasterResponse(_,_) =>
            bestMaster
        }
      }
    }
    newMaster
  }

  /**
   * Voting new master for concrete partition
   * @param partition Partition to vote new master
   * @return New master Address
   */
  protected def startVoting(partition : Int) : String = {
    val lock = zkService.getLock(s"/producers/lock_voting/$streamName/$partition")
    lock.lock()
    val newMaster = startVotingInternal(partition)
    lock.unlock()
    newMaster
  }

  /**
   * Updating master on concrete partition
   * @param partition Partition to update master
   * @param init If flag true master will be reselected anyway else old master can stay
   * @param retries Retries to try to interact with master
   */
  protected def updateMaster(partition : Int, init : Boolean, retries : Int = zkRetriesAmount) : Unit = {
    val masterOpt = getMaster(partition)
    if (masterOpt.isDefined) {
      val master = masterOpt.get
      if (init) {
        val ans = transport.deleteMasterRequest(DeleteMasterRequest(agentAddress, master, partition), transportTimeout)
        ans match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            updateMaster(partition, init, retries-1)

          case EmptyResponse(_, _) =>
            Thread.sleep(1000)
            updateMaster(partition, init, zkRetriesAmount)

          case DeleteMasterResponse(_, _) =>
            val newMaster = startVoting(partition)
            lockLocalMasters.lock()
            localMasters(partition) = newMaster
            lockLocalMasters.unlock()
        }

      } else {
        transport.pingRequest(PingRequest(agentAddress, master, partition), transportTimeout) match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            updateMaster(partition, init, retries-1)

          case EmptyResponse(_,_) =>
            Thread.sleep(1000)
            updateMaster(partition, init, zkRetriesAmount)

          case PingResponse(_,_) =>
            lockLocalMasters.lock()
            localMasters(partition) = master
            lockLocalMasters.unlock()
        }
      }
    }
    else {
      startVoting(partition)
    }
  }

  /**
   * Return master for concrete partition
   * @param partition Partition to set
   * @return Master address
   */
  protected def getMaster(partition : Int) : Option[String] = {
    lockManagingMaster.lock()
    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
    lock.lock()
    val masterOpt = zkService.get[String](s"/producers/master/$streamName/$partition")
    lock.unlock()
    lockManagingMaster.unlock()
    masterOpt
  }

  /**
   * Set this agent as new master on concrete partition
   * @param partition Partition to set
   */
  protected def setThisAgentAsMaster(partition : Int) : Unit = {
    lockManagingMaster.lock()
    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
    lock.lock()
    //TODO remove after debug
    assert(!zkService.exist(s"/producers/master/$streamName/$partition"))
    zkService.create(s"/producers/master/$streamName/$partition", agentAddress, CreateMode.EPHEMERAL)
    lock.unlock()
    lockManagingMaster.unlock()
  }

  /**
   * Unset this agent as master on concrete partition
   * @param partition Partition to set
   */
  protected def deleteThisAgentFromMasters(partition : Int) : Unit = {
    lockManagingMaster.lock()
    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
    lock.lock()
    zkService.delete(s"/producers/master/$streamName/$partition")
    lock.unlock()
    lockManagingMaster.unlock()
  }

  /**
   * Starting validate zk connection(if it will be down, exception will be thrown)
   */
  protected def startValidator() = {
    val latch = new CountDownLatch(1)
    val zkConnectionValidator = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        var retries = 0
        while (true) {
          if (!zkService.isZkConnected)
            retries += 1
          else
            retries = 0
          if (retries >= 3)
            throw new IllegalStateException("zookeeper connection lost")
          Thread.sleep(1000)
        }
      }
    })
    zkConnectionValidator.start()
    latch.await()
  }

  /**
   * Retrieve new transaction from agent
   * @param partition Transaction partition
   * @return Transaction UUID
   */
  def getNewTxn(partition : Int) : UUID = {
    lockLocalMasters.lock()
    val condition = localMasters.contains(partition)
    val localMaster = if (condition) localMasters(partition) else null
    lockLocalMasters.unlock()
    if (condition){
      val txnResponse = transport.transactionRequest(new TransactionRequest(agentAddress, localMaster, partition), transportTimeout)
      txnResponse match {
        case null =>
          updateMaster(partition, init = false)
          getNewTxn(partition)

        case EmptyResponse(snd,rcv) =>
          updateMaster(partition, init = false)
          getNewTxn(partition)

        case TransactionResponse(snd, rcv, uuid) =>
          uuid
      }
    } else {
      updateMaster(partition, init = false)
      getNewTxn(partition)
    }
  }

  /**
   * Start handling incoming messages for this agent
   */
  protected def startHandleMessages() = {
    val latch = new CountDownLatch(1)
    val messageHandler = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        while (true) {
          val request: IMessage = transport.waitRequest()
          request match {
            case PingRequest(snd, rcv, partition) =>
              lockLocalMasters.lock()
              //TODO remove after debug
              assert(rcv == agentAddress)
              val response = {
                if (localMasters.contains(partition) && localMasters(partition) == agentAddress)
                  PingResponse(rcv, snd)
                else
                  EmptyResponse(rcv, snd)
              }
              lockLocalMasters.unlock()
              response.msgID = request.msgID
              transport.response(response)

            case SetMasterRequest(snd, rcv, partition) =>
              lockLocalMasters.lock()
              //TODO remove after debug
              assert(rcv == agentAddress)
              localMasters(partition) = agentAddress
              setThisAgentAsMaster(partition)
              usedPartitions foreach { partition =>
                updateThisAgentPriority(partition, value = -1)
              }
              val response = SetMasterResponse(rcv, snd)
              lockLocalMasters.unlock()
              response.msgID = request.msgID
              transport.response(response)

            case DeleteMasterRequest(snd, rcv, partition) =>
              lockLocalMasters.lock()
              //TODO remove after debug
              assert(rcv == agentAddress)
              val response = {
                if (localMasters.contains(partition) && localMasters(partition) == agentAddress) {
                  localMasters.remove(partition)
                  deleteThisAgentFromMasters(partition)
                  usedPartitions foreach { partition =>
                    updateThisAgentPriority(partition, value = 1)
                  }
                  DeleteMasterResponse(rcv, snd)
                } else
                  EmptyResponse(rcv, snd)
              }
              lockLocalMasters.unlock()
              response.msgID = request.msgID
              transport.response(response)

            case TransactionRequest(snd, rcv, partition) =>
              lockLocalMasters.lock()
              //TODO remove after debug
              assert(rcv == agentAddress)
              val response = {
                if (localMasters.contains(partition) && localMasters(partition) == agentAddress) {
                  val txnUUID: UUID = producer.getLocalTxn(partition)
                  TransactionResponse(rcv, snd, txnUUID)
                } else
                  EmptyResponse(rcv, snd)
              }
              lockLocalMasters.unlock()
              response.msgID = request.msgID
              transport.response(response)
          }
        }
      }
    })
    messageHandler.start()
    latch.await()
  }
}