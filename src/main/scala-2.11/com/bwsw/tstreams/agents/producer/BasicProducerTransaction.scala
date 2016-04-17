package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.coordination.{ProducerTransactionStatus, ProducerTopicMessage}
import com.typesafe.scalalogging.Logger
import org.redisson.core.{RTopic, RLock}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.Breaks._
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Transaction retrieved by BasicProducer.newTransaction method
 * @param partition Concrete partition for saving this transaction
 * @param basicProducer Producer class which was invoked newTransaction method
 * @tparam USERTYPE User data type
 * @tparam DATATYPE Storage data type
 */
class BasicProducerTransaction[USERTYPE,DATATYPE](partition : Int,
                                                  basicProducer: BasicProducer[USERTYPE,DATATYPE]){

  /**
   * BasicProducerTransaction logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  logger.info(s"Open transaction for stream,partition : {${basicProducer.stream.getName}},{$partition}\n")

  /**
   * Return transaction partition
   */
  def getPartition : Int = partition

  /**
   * Return transaction UUID
   */
  def getTxnUUID: UUID = transactionUuid

  /**
   * Variable for indicating transaction state
   */
  private var closed = false

  /**
   * Transaction part index
   */
  private var part = 0

  /**
   * Max awaiting time on Await.ready() method for futures
   */
  private val TIMEOUT = 2 seconds

  /**
   * All inserts (can be async) in storage (must be waited before closing this transaction)
   */
  private var jobs = ListBuffer[() => Unit]()

  /**
   * Queue to figure out moment when transaction is going to close
   */
  private val updateQueue = new LinkedBlockingQueue[Boolean](10)

  /**
   * Json serializer inst
   */
  private val jsonSerializer = new JsonSerializer

  /**
   * Topic reference for concrete producer on concrete stream/partition to publish events(cancel,close,update)
   */
  private val topicRef: RTopic[String] =
    basicProducer.stream.coordinator.getTopic[String](s"${basicProducer.stream.getName}/$partition/events")

  /**
   * Lock reference for concrete producer on concrete stream/partition
   */
  private val lockRef: RLock = basicProducer.stream.coordinator.getLock(s"${basicProducer.stream.getName}/$partition")

  lockRef.lock()

  private val transactionUuid = basicProducer.producerOptions.txnGenerator.getTimeUUID()

  basicProducer.stream.metadataStorage.commitEntity.commit(
    streamName = basicProducer.stream.getName,
    partition = partition,
    transaction = transactionUuid,
    totalCnt = -1,
    ttl = basicProducer.producerOptions.transactionTTL)

  topicRef.publish(
    jsonSerializer.serialize(
    ProducerTopicMessage(
    txnUuid = transactionUuid,
    ttl = basicProducer.producerOptions.transactionTTL,
    status = ProducerTransactionStatus.opened)))

  lockRef.unlock()

  /**
   * Future to keep this transaction alive
   */
  private val updateFuture: Future[Unit] = startAsyncKeepAlive(
    basicProducer.stream.getName,
    partition,
    transactionUuid,
    basicProducer.producerOptions.transactionTTL,
    basicProducer.producerOptions.transactionKeepAliveInterval)

  /**
   * Send data to storage
   * @param obj some user object
   */
  def send(obj : USERTYPE) : Unit = {
    if (closed)
      throw new IllegalStateException("transaction is closed")

    basicProducer.producerOptions.insertType match {
      case InsertionType.BatchInsert(size) =>
        basicProducer.stream.dataStorage.putInBuffer(
          basicProducer.stream.getName,
          partition,
          transactionUuid,
          basicProducer.stream.getTTL,
          basicProducer.producerOptions.converter.convert(obj),
          part)
        if (basicProducer.stream.dataStorage.getBufferSize() == size) {
          val job: () => Unit = basicProducer.stream.dataStorage.saveBuffer()
          if (job != null)
            jobs += job
          basicProducer.stream.dataStorage.clearBuffer()
        }

      case InsertionType.SingleElementInsert =>
        val job: () => Unit = basicProducer.stream.dataStorage.put(
          basicProducer.stream.getName,
          partition,
          transactionUuid,
          basicProducer.stream.getTTL,
          basicProducer.producerOptions.converter.convert(obj),
          part)
        if (job != null)
          jobs += job

      case _ =>
        throw new IllegalStateException("InsertType can't be resolved")
    }

    part += 1
  }

  /**
   * Canceling current transaction
   */
  def cancel() = {
    if (closed)
      throw new IllegalStateException("transaction is already closed")

    basicProducer.producerOptions.insertType match {
      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(_) =>
        basicProducer.stream.dataStorage.clearBuffer()

      case _ =>
        throw new IllegalStateException("Insert Type can't be resolved")
    }

    jobs.foreach(x=>x()) // wait all async jobs done before commit

    updateQueue.put(true)

    //await till update future will close
    Await.ready(updateFuture, TIMEOUT)

    topicRef.publish(
      jsonSerializer.serialize(
        ProducerTopicMessage(
          txnUuid = transactionUuid,
          ttl = -1,
          status = ProducerTransactionStatus.canceled)))

    closed = true
    logger.info(s"Cancel transaction for stream,partition : {${basicProducer.stream.getName}},{$partition}\n")
  }

  /**
   * Submit transaction(transaction will be available by consumer only after closing)
   */
  def close() : Unit = {
    if (closed)
      throw new IllegalStateException("transaction is already closed")

    basicProducer.producerOptions.insertType match {
      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(size) =>
        if (basicProducer.stream.dataStorage.getBufferSize() > 0) {
          val job: () => Unit = basicProducer.stream.dataStorage.saveBuffer()
          if (job != null)
            jobs += job
          basicProducer.stream.dataStorage.clearBuffer()
        }

      case _ =>
        throw new IllegalStateException("Insert Type can't be resolved")
    }

    jobs.foreach(x => x()) // wait all async jobs done before commit

    updateQueue.put(true)

    //await till update future will close
    Await.ready(updateFuture, TIMEOUT)

    //close transaction using stream ttl
    if (part > 0) {
      basicProducer.stream.metadataStorage.commitEntity.commit(
        streamName = basicProducer.stream.getName,
        partition = partition,
        transaction = transactionUuid,
        totalCnt = part,
        ttl = basicProducer.stream.getTTL)

      //publish that current txn is closed
      topicRef.publish(
        jsonSerializer.serialize(
        ProducerTopicMessage(
        txnUuid = transactionUuid,
        ttl = -1,
        status = ProducerTransactionStatus.closed)))
    }

    closed = true
    logger.info(s"Close transaction for stream,partition : {${basicProducer.stream.getName}},{$partition}\n")
  }

  /**
   * State indicator of the transaction
   * @return Closed transaction or not
   */
  def isClosed = closed

  /**
   * Async job for keeping alive current transaction
   * @param streamName Stream name of current transaction
   * @param partition Partition of current transaction
   * @param transaction Current transaction UUID
   * @param ttl Transaction live time in seconds
   * @param keepAliveInterval Ttl update frequency in seconds
   */
  private def startAsyncKeepAlive(streamName : String,
                                  partition : Int,
                                  transaction : UUID,
                                  ttl : Int,
                                  keepAliveInterval : Int) : Future[Unit] = {

    Future {
      breakable { while (true) {

        val value = updateQueue.poll(keepAliveInterval * 1000, TimeUnit.MILLISECONDS)
        if (value)
          break()

        //-1 here indicate that transaction is started but not finished yet
        basicProducer.stream.metadataStorage.producerCommitEntity.commit(
          streamName = streamName,
          partition = partition,
          transaction = transaction,
          totalCnt = -1,
          ttl = ttl)

        //publish that current txn is being updating
        topicRef.publish(jsonSerializer.serialize(ProducerTopicMessage(
          txnUuid = transactionUuid,
          ttl = ttl,
          status = ProducerTransactionStatus.opened)))
      }}
    }
  }
}