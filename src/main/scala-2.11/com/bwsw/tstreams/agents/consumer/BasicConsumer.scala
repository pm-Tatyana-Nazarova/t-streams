package com.bwsw.tstreams.agents.consumer

import java.util.UUID
import com.bwsw.tstreams.agents.group.{ConsumerCommitInfo, CommitInfo, Agent}
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.entities.TransactionSettings
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.BasicStream
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


/**
 * Basic consumer class
 * @param name Name of consumer
 * @param stream Stream from which to consume transactions
 * @param options Basic consumer options
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
class BasicConsumer[DATATYPE, USERTYPE](val name : String,
                                        val stream : BasicStream[DATATYPE],
                                        val options : BasicConsumerOptions[DATATYPE, USERTYPE]) extends Agent{


  /**
   * BasicConsumer logger for logging
   */
    private val logger = Logger(LoggerFactory.getLogger(this.getClass))

    logger.info(s"Start new Basic consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

  /**
   * Temporary checkpoints (will be cleared after every checkpoint() invokes)
   */
    private val offsetsForCheckpoint = scala.collection.mutable.Map[Int, UUID]()

  /**
   * Local offsets
   */
    protected val currentOffsets = scala.collection.mutable.Map[Int, UUID]()

  /**
   * Indicate set offsets or not
   */
    private var isSet = false

    //set consumer offsets
    if(!stream.metadataStorage.consumerEntity.exist(name) || !options.useLastOffset){
      isSet = true

      options.offset match {
        case Offsets.Oldest =>
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = options.txnGenerator.getTimeUUID(0)
            offsetsForCheckpoint(i) = options.txnGenerator.getTimeUUID(0)
          }

        case Offsets.Newest =>
          val newestUuid = options.txnGenerator.getTimeUUID()
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = newestUuid
            offsetsForCheckpoint(i) = newestUuid
          }

        case dateTime : Offsets.DateTime =>
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = options.txnGenerator.getTimeUUID(dateTime.startTime.getTime)
            offsetsForCheckpoint(i) = options.txnGenerator.getTimeUUID(dateTime.startTime.getTime)
          }

        case offset : Offsets.UUID =>
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = offset.startUUID
            offsetsForCheckpoint(i) = offset.startUUID
          }

        case _ => throw new IllegalStateException("offset cannot be resolved")
      }

    }

    if (!isSet) {
      for (i <- 0 until stream.getPartitions) {
        val offset = stream.metadataStorage.consumerEntity.getOffset(name, stream.getName, i)
        offsetsForCheckpoint(i) = offset
        currentOffsets(i) = offset
      }
    }

  /**
   * Buffer for transactions preload
   */
    private val transactionBuffer = scala.collection.mutable.Map[Int, scala.collection.mutable.Queue[TransactionSettings]]()
    //fill transaction buffer using current offsets
    for (i <- 0 until stream.getPartitions)
      transactionBuffer(i) = stream.metadataStorage.commitEntity.getTransactionsMoreThan(
        stream.getName,
        i,
        currentOffsets(i),
        options.transactionsPreload)

  /**
   * Helper function for getTransaction() method
   * @return BasicConsumerTransaction or None
   */
    private def getTxnOpt : Option[BasicConsumerTransaction[DATATYPE,USERTYPE]] = {
      if (options.readPolicy.isRoundFinished())
        return None

      val curPartition = options.readPolicy.getNextPartition

      if (transactionBuffer(curPartition).isEmpty) {
        transactionBuffer(curPartition) = stream.metadataStorage.commitEntity.getTransactionsMoreThan(
          stream.getName,
          curPartition,
          currentOffsets(curPartition),
          options.transactionsPreload)
      }

      if (transactionBuffer(curPartition).isEmpty)
        return getTxnOpt

      val txn: TransactionSettings = transactionBuffer(curPartition).front

      if (txn.totalItems != -1) {
        offsetsForCheckpoint(curPartition) = txn.txnUuid
        currentOffsets(curPartition) = txn.txnUuid
        transactionBuffer(curPartition).dequeue()
        return Some(new BasicConsumerTransaction[DATATYPE, USERTYPE](this, curPartition, txn))
      }

      val updatedTxnOpt: Option[TransactionSettings] = updateTransaction(txn.txnUuid, curPartition)

      if (updatedTxnOpt.isDefined) {
        val updatedTxn = updatedTxnOpt.get

        if (updatedTxn.totalItems != -1) {
          offsetsForCheckpoint(curPartition) = txn.txnUuid
          currentOffsets(curPartition) = txn.txnUuid
          transactionBuffer(curPartition).dequeue()
          return Some(new BasicConsumerTransaction[DATATYPE, USERTYPE](this, curPartition, updatedTxn))
        }
      }
      else
        transactionBuffer(curPartition).dequeue()

      getTxnOpt
    }

  /**
   * @return Consumed transaction of None if nothing to consume
   */
    def getTransaction: Option[BasicConsumerTransaction[DATATYPE, USERTYPE]] = {
      logger.info(s"Start new transaction for consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

      options.readPolicy.startNewRound()
      val txn: Option[BasicConsumerTransaction[DATATYPE, USERTYPE]] = getTxnOpt
      txn
    }

  /**
   * Getting last transaction from concrete partition
   * @param partition Partition to get last transaction
   * @return Last txn
   */
    def getLastTransaction(partition : Int): Option[BasicConsumerTransaction[DATATYPE, USERTYPE]] = {
      var now = options.txnGenerator.getTimeUUID()
      var done = false
      while(!done){
        val queue = stream.metadataStorage.commitEntity.getLastTransactionHelper(
          stream.getName,
          partition,
          now)
        if (queue.isEmpty)
          done = true
        else {
          while (queue.nonEmpty) {
            val txn = queue.dequeue()
            if (txn.totalItems != -1)
              return Some(new BasicConsumerTransaction[DATATYPE, USERTYPE](this, partition, txn))
            now = txn.txnUuid
          }
        }
      }

      None
    }

  /**
   *
   * @param partition Partition from which historic transaction will be retrieved
   * @param uuid Uuid for this transaction
   * @return BasicConsumerTransaction
   */
    def getTransactionById(partition : Int, uuid : UUID): Option[BasicConsumerTransaction[DATATYPE, USERTYPE]] = {
      logger.info(s"Start new historic transaction for consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")
      val txnOpt = updateTransaction(uuid, partition)
      if (txnOpt.isDefined){
        val txn = txnOpt.get
        if (txn.totalItems != -1)
          Some(new BasicConsumerTransaction[DATATYPE,USERTYPE](this, partition, txn))
        else
          None
      }
      else
        None
    }

  /**
   * Sets offset on concrete partition
   * @param partition partition to set offset
   * @param uuid offset value
   */
    def setLocalOffset(partition : Int, uuid : UUID) : Unit = {
      offsetsForCheckpoint(partition) = uuid
      currentOffsets(partition) = uuid
      transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactionsMoreThan(
        stream.getName,
        partition,
        uuid,
        options.transactionsPreload)
    }

  /**
   * Update transaction (if transaction is not closed it will have total packets value -1 so we need to wait while it will close)
   * @param txn Transaction to update
   * @return Updated transaction
   */
    private def updateTransaction(txn : UUID, partition : Int) : Option[TransactionSettings] = {
      val amount: Option[(Int,Int)] = stream.metadataStorage.commitEntity.getTransactionAmount(
        stream.getName,
        partition,
        txn)
      if (amount.isDefined) {
        val (cnt, ttl) = amount.get
        Some(TransactionSettings(txn, cnt, ttl))
      }
      else
        None
    }

  /**
   * Save current offsets in metadata to read later from them (in case of system stop/failure)
   */
    def checkpoint() : Unit = {
      logger.info(s"Start saving checkpoints for consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}\n")

      stream.metadataStorage.consumerEntity.saveBatchOffset(name, stream.getName, offsetsForCheckpoint)
      offsetsForCheckpoint.clear()
    }

  /**
   * Info to commit
   */
  override def getCommitInfo(): List[CommitInfo] = {
    val info = ListBuffer[CommitInfo]()
    offsetsForCheckpoint.foreach{case(partition, lastTxn) =>
        info += ConsumerCommitInfo(name, stream.getName, partition, lastTxn)
    }
    offsetsForCheckpoint.clear()
    info.toList
  }

  /**
   * @return Metadata storage link for concrete agent
   */
  override def getMetadataRef(): MetadataStorage = stream.metadataStorage

  /**
   * @return Coordinator link
   */
  override def getCoordinationRef(): Coordinator = stream.coordinator
}