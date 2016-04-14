package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import com.bwsw.tstreams.lockservice.traits.ILocker
import com.typesafe.scalalogging.Logger
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
   * Locker reference for concrete producer on concrete stream/partition
   */
  private val lockerRef: ILocker = basicProducer.stream.lockService.getLocker(s"/${basicProducer.stream.getName}/$partition")


  lockerRef.lock()

  private val transaction = basicProducer.producerOptions.txnGenerator.getTimeUUID()
  basicProducer.stream.metadataStorage.commitEntity.commit(
    basicProducer.stream.getName,
    partition,
    transaction,
    totalCnt = -1,
    ttl = basicProducer.stream.getTTL)

  lockerRef.unlock()

  /**
   * Future to keep this transaction alive
   */
  private val updateFuture: Future[Unit] = startAsyncKeepAlive(
    basicProducer.stream.getName,
    partition,
    transaction,
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
          transaction,
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
          transaction,
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
        basicProducer.stream.getName,
        partition,
        transaction,
        part,
        basicProducer.stream.getTTL)
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
            streamName,
            partition,
            transaction,
            totalCnt = -1,
            ttl)

      }}
    }
  }
}