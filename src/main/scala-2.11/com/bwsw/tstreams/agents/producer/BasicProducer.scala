package com.bwsw.tstreams.agents.producer

import java.util.UUID
import com.bwsw.tstreams.agents.group.{CommitInfo, Agent}
import com.bwsw.tstreams.agents.producer.ProducerPolicies.ProducerPolicy
import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.bwsw.tstreams.interaction.subscribe.messages.{ProducerTransactionStatus, ProducerTopicMessage}
import com.bwsw.tstreams.interaction.transactions.PeerToPeerAgent
import com.bwsw.tstreams.interaction.transactions.transport.traits.Interaction
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.BasicStream
import org.redisson.core.RTopic
import org.slf4j.LoggerFactory

/**
 * Basic producer class
 * @param name Producer name
 * @param stream Stream for transaction sending
 * @param producerOptions This producer options
 * @tparam USERTYPE User data type
 * @tparam DATATYPE Storage data type
 */
class BasicProducer[USERTYPE,DATATYPE](val name : String,
                                       val stream : BasicStream[DATATYPE],
                                       val producerOptions: BasicProducerOptions[USERTYPE,DATATYPE]) extends Agent with Interaction{

  stream.dataStorage.bind()

  private val serializer = new JsonSerializer

  /**
   * BasicProducer logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

  /**
   * Map for memorize opened transaction on partitions
   */
  private val mapPartitions = scala.collection.mutable.Map[Int, BasicProducerTransaction[USERTYPE,DATATYPE]]()

  /**
   * @param policy Policy for previous transaction on concrete partition
   * @param nextPartition Next partition to use for transaction (default -1 which mean that write policy will be used)
   * @return BasicProducerTransaction instance
   */
  def newTransaction(policy: ProducerPolicy, nextPartition : Int = -1) : BasicProducerTransaction[USERTYPE,DATATYPE] = {

    val partition = {
      if (nextPartition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        nextPartition
    }

    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException("invalid partition")

    logger.debug(s"Start new BasicProducerTransaction for BasicProducer " +
      s"with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions} on partition $partition\n")

    val transaction = {
      val txnUUID = agent.getNewTxn(partition)
      val txn = new BasicProducerTransaction[USERTYPE, DATATYPE](partition, txnUUID, this)
      if (mapPartitions.contains(partition)) {
        val prevTxn = mapPartitions(partition)
        if (!prevTxn.isClosed) {
          policy match {
            case ProducerPolicies.checkpointIfOpen =>
              prevTxn.checkpoint()

            case ProducerPolicies.cancelIfOpen =>
              prevTxn.cancel()

            case ProducerPolicies.errorIfOpen =>
              throw new IllegalStateException("previous transaction was not closed")
          }
        }
      }
      mapPartitions(partition) = txn
      txn
    }

    transaction
  }

  /**
   * Return reference for transaction from concrete partition
   * @param partition Partition from which transaction will be retrieved
   * @return Transaction reference if it exist or not closed
   */
  def getTransaction(partition : Int) : Option[BasicProducerTransaction[USERTYPE,DATATYPE]] = {
    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException("invalid partition")
    if (mapPartitions.contains(partition)) {
      val txn = mapPartitions(partition)
      if (txn.isClosed)
        return None
      Some(txn)
    }
    else
      None
  }

  /**
   * Close all opened transactions
   */
  def checkpoint() : Unit = {
    mapPartitions.map{case(partition,txn)=>txn}.foreach{ x=>
      if (!x.isClosed)
        x.checkpoint()
    }
  }

  /**
   * Info to commit
   */
  //TODO implement getting commit info from transactions
  override def getCommitInfo(): List[CommitInfo] = {
    checkpoint()
    List()
  }

  /**
   * @return Metadata storage link for concrete agent
   */
  override def getMetadataRef(): MetadataStorage = stream.metadataStorage

  /**
   * Method to implement for concrete producer [[PeerToPeerAgent]] method
   * Need only if this producer is master
   * @return UUID
   */
  override def getLocalTxn(partition : Int): UUID = {
    val topicRef: RTopic[String] = stream.coordinator.getTopic[String](s"${stream.getName}/$partition/events")
    val transactionUuid = producerOptions.txnGenerator.getTimeUUID()

    stream.metadataStorage.commitEntity.commit(
      streamName = stream.getName,
      partition = partition,
      transaction = transactionUuid,
      totalCnt = -1,
      ttl = producerOptions.transactionTTL)

    val msg = ProducerTopicMessage(
      txnUuid = transactionUuid,
      ttl = producerOptions.transactionTTL,
      status = ProducerTransactionStatus.opened)

    topicRef.publish(serializer.serialize(msg))
    transactionUuid
  }

  /**
   * P2P Agent for producers interaction
   */
  override val agent: PeerToPeerAgent = new PeerToPeerAgent(
    agentAddress = producerOptions.peerToPeerAgentSettings.agentAddress,
    zkHosts = producerOptions.peerToPeerAgentSettings.zkHosts,
    zkRootPath = producerOptions.peerToPeerAgentSettings.zkRootPath,
    zkTimeout = producerOptions.peerToPeerAgentSettings.zkTimeout,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartition(),
    isLowPriorityToBeMaster = producerOptions.peerToPeerAgentSettings.isLowPriorityToBeMaster,
    transport = producerOptions.peerToPeerAgentSettings.transport,
    transportTimeout = producerOptions.peerToPeerAgentSettings.transportTimeout)
}
