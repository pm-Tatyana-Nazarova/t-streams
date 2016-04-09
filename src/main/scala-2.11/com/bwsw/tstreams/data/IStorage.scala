package com.bwsw.tstreams.data

/**
  * Interface for data storage
  * @tparam T Storage data type
  */
trait IStorage[T] {

  /**
   * @return Closed concrete storage or not
   */
  def isClosed() : Boolean

  /**
   * @return Correctness of created data storage(not supported now)
   */
  @deprecated("now not available","1.0")
  def validate() : Boolean

  /**
   * Initialize data storage
   */
  def init()

  /**
   * Remove all data in data storage
   */
  def truncate()

  /**
   * Close storage
   */
  def close()

  /**
   * Remove storage
   */
  def remove()

  /**
   * Put data in storage
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param data Data which will be put
   * @param partNum Data unique number
   * @param ttl Time of records expiration in seconds
   * @return Lambda which indicate done or not putting request(if request was async) null else
   */
  def put(streamName : String, partition : Int, transaction : java.util.UUID, ttl : Int, data : T, partNum : Int) : () => Unit

  /**
   * Get data from storage
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param from Data unique number from which reading will start
   * @param to Data unique number from which reading will stop
   * @return Queue of object which have storage type
   */
  def get(streamName : String, partition : Int, transaction : java.util.UUID, from : Int, to : Int) : scala.collection.mutable.Queue[T]


  /**
   * Put data in buffer to save it later
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param data Data which will be put
   * @param partNum Data unique number
   * @param ttl Time of records expiration in seconds
   */
  def putInBuffer(streamName : String, partition : Int, transaction : java.util.UUID, ttl : Int, data : T, partNum : Int) : Unit


  /**
   * Save all info from buffer in IStorage
   * @return Lambda which indicate done or not putting request(if request was async) null else
   */
  def saveBuffer() : () => Unit
}