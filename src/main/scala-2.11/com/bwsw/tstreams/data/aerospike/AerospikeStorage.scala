package com.bwsw.tstreams.data.aerospike

import java.util.UUID

import com.aerospike.client.{AerospikeClient, Bin, Key, Record}
import com.bwsw.tstreams.data.IStorage
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future


/**
 * Aerospike storage impl of IStorage
 * @param options User defined aerospike options
 */
class AerospikeStorage(options : AerospikeStorageOptions) extends IStorage[Array[Byte]]{

  /**
   * AerospikeStorage logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Aerospike client
   */
  private val client = new AerospikeClient(options.policy,options.hosts:_*)

  /**
   * @return Closed concrete storage or not
   */
  override def isClosed(): Boolean =
    client.isConnected

  /**
   * Initialize data storage
   */
  override def init(): Unit = {
    //TODO review
    logger.warn("this method was not implemented for aerospike")
  }

  /**
   * Get data from storage
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param from Data unique number from which reading will start
   * @param to Data unique number from which reading will stop
   * @return Queue of object which have storage type
   */
  override def get(streamName: String, partition: Int, transaction: UUID, from: Int, to: Int): mutable.Queue[Array[Byte]] = {
    val key : Key = new Key(options.namespace, s"$streamName/$partition", transaction.toString)
    val names = (from to to).toList.map(x=>x.toString)
    logger.debug(s"Start retrieving data from aerospike for streamName: {$streamName}, partition: {$partition}")
    val record: Record = client.get(options.readPolicy, key, names:_*)
    logger.debug(s"Finished retrieving data from aerospike for streamName: {$streamName}, partition: {$partition}")

    val data = scala.collection.mutable.Queue[Array[Byte]]()
    for (name <- names){
      data.enqueue(record.getValue(name).asInstanceOf[Array[Byte]])
    }
    data
  }

  /**
   * Put data in storage
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param data Data which will be put
   * @param partNum Data unique number
   * @param ttl Time of records expiration in seconds
   * @return Null instead of wait lambda because client.put is not async
   */
  override def put(streamName: String, partition: Int, transaction: UUID, ttl: Int, data: Array[Byte], partNum: Int): () => Unit = {
    options.writePolicy.expiration = ttl
    val key: Key = new Key(options.namespace, s"$streamName/$partition", transaction.toString)
    val bin = new Bin(partNum.toString, data)
    logger.debug(s"Start putting data in aerospike for streamName: {$streamName}, partition: {$partition}")
    client.put(options.writePolicy, key, bin)
    logger.debug(s"Finished putting data in aerospike for streamName: {$streamName}, partition: {$partition}")
    null
  }

  /**
   * Remove all data in data storage
   */
  override def truncate(): Unit = {
    //TODO review
    logger.warn("this method was not implemented for aerospike")
  }

  /**
   * Remove storage
   */
  override def remove(): Unit = {
    //TODO review
    logger.warn("this method was not implemented for aerospike")
  }

  /**
   * @return Correctness of created data storage(not supported now)
   */
  override def validate(): Boolean = ???

  /**
   * Close storage
   */
  override def close(): Unit =
    client.close()

  /**
   * Save all info from buffer in IStorage
   * @return Lambda which indicate done or not putting request(if request was async) null else
   */
  override def saveBuffer(): () => Unit = {
    val elem = buffer.head
    options.writePolicy.expiration = elem.ttl
    val key: Key = new Key(options.namespace, s"${elem.streamName}/${elem.partition}", elem.transaction.toString)

    val mapped = buffer map {elem =>
      new Bin(elem.partNum.toString, elem.data)
    }

    logger.debug(s"Start putting batch of data with size:${getBufferSize()} in aerospike for streamName: {${elem.streamName}}, partition: {${elem.partition}")
    client.put(options.writePolicy, key, mapped:_*)
    logger.debug(s"Finished putting batch of data with size:${getBufferSize()} in aerospike for streamName: {${elem.streamName}}, partition: {${elem.partition}")

    null
  }
}