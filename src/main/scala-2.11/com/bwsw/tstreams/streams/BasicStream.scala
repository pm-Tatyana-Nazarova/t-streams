package com.bwsw.tstreams.streams

import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.lockservice.traits.ILockerFactory
import com.bwsw.tstreams.metadata.MetadataStorage
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * Stream instance
 * @param name Name of the stream
 * @param partitions Number of stream partitions
 * @param metadataStorage Stream metadata storage which it used
 * @param dataStorage Data storage which will be using stream
 * @param ttl Time of transaction time expiration in seconds
 * @param description Some additional info about stream
 * @tparam T Storage data type
 */
class BasicStream[T](val name : String,
                  private var partitions : Int,
                  val metadataStorage: MetadataStorage,
                  val dataStorage : IStorage[T],
                  val lockService: ILockerFactory,
                  private var ttl : Int,
                  private var description : String){

  /**
   * Transaction minimum ttl time
   */
  private val minTxnTTL = 3

  if (ttl < minTxnTTL)
    throw new IllegalArgumentException(s"ttl should be greater or equal than $minTxnTTL")

  /**
   * Basic Stream logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Create lockers for all partitions if path already exist nothing will happen
   */
  if (lockService != null) {
    //TODO mb special case for consumer
    for (partition <- 0 until partitions)
      lockService.createLocker(s"/$name/$partition")
  }

  /**
   * @return Name
   */
  def getName =
    name

  /**
   * @return Number of partitions
   */
  def getPartitions =
    partitions

  /**
   * @return TTL
   */
  def getTTL =
    ttl

  /**
   * @return Description
   */
  def getDescriptions =
    description

  /**
   * Save stream info in metadata
   */
  def save() : Unit = {
    if (lockService != null) {
      //TODO mb special case for consumer
      for (partition <- 0 until partitions)
        lockService.createLocker(s"/$name/$partition")
    }
    logger.info(s"start alternating stream with name : {$name}, partitions : {$partitions}, ttl : {$ttl}, description : {$description}\n")
    metadataStorage.streamEntity.alternateStream(name, partitions, ttl, description)
    logger.info(s"finished alternating stream with name : {$name}, partitions : {$partitions}, ttl : {$ttl}, description : {$description}\n")
  }

  /**
   * @param value New partitions value
   */
  def setPartitions(value : Int) : Unit =
    partitions = value


  /**
   * @param value Some additional info about stream
   */
  def setDescription(value : String) : Unit =
    description = value

  /**
   * @param value New TTL value
   */
  def setTTL(value : Int) : Unit =
    ttl = value

}
