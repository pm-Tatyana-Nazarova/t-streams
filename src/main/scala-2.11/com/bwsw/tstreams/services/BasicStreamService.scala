package com.bwsw.tstreams.services

import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.entities.StreamSettings


import com.bwsw.tstreams.lockservice.traits.ILockerFactory
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.BasicStream
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
 * Service for streams
 */
object BasicStreamService {

  /**
   * Basic Stream logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Getting existing stream
   * @param streamName Name of the stream
   * @param metadataStorage Metadata storage of concrete stream
   * @param dataStorage Data storage of concrete stream
   * @return Stream instance
   * @tparam T Type of stream data
   */
  def loadStream[T](streamName : String,
                    metadataStorage: MetadataStorage,
                    dataStorage: IStorage[T],
                    lockService: ILockerFactory) : BasicStream[T] = {


    logger.info(s"start load stream with name : {$streamName}\n")
    val settingsOpt: Option[StreamSettings] = metadataStorage.streamEntity.getStream(streamName)

    logger.info(s"finished load stream with name : {$streamName}\n")
    if (settingsOpt.isEmpty)
      throw new IllegalArgumentException("stream with this name can not be loaded")
    else {
      val settings = settingsOpt.get
      val (name: String, partitions: Int, ttl: Int, description: String) =
        (settings.name,settings.partitions,settings.ttl,settings.description)

      val stream: BasicStream[T] = new BasicStream(name, partitions, metadataStorage, dataStorage, lockService, ttl, description)
      stream
    }
  }

  /**
   * Creating stream
   * @param streamName Name of the stream
   * @param partitions Number of stream partitions
   * @param metadataStorage Metadata storage using by this stream
   * @param dataStorage Data storage using by this stream
   * @param description Some additional info about stream
   * @param ttl Expiration time of single transaction in seconds
   * @tparam T Type of stream data
   */
  def createStream[T](streamName : String,
                   partitions : Int,
                   ttl : Int,
                   description : String,
                   metadataStorage: MetadataStorage,
                   dataStorage : IStorage[T],
                   lockService: ILockerFactory) : BasicStream[T] = {


    logger.info(s"start stream creation with name : {$streamName}, {$partitions}, {$ttl}\n")
    metadataStorage.streamEntity.createStream(streamName,partitions,ttl,description)

    logger.info(s"finished stream creation with name : {$streamName}, {$partitions}, {$ttl}\n")
    val stream: BasicStream[T] = new BasicStream[T](streamName,partitions,metadataStorage, dataStorage, lockService, ttl, description)

    stream
  }


  /**
   * Deleting concrete stream
   * @param streamName Name of the stream to delete
   * @param metadataStorage Name of metadata storage where concrete stream exist
   */
  def deleteStream(streamName : String, metadataStorage: MetadataStorage) : Unit = {
    logger.info(s"start deleting stream with name : $streamName\n")
    metadataStorage.streamEntity.deleteStream(streamName)
    logger.info(s"finished deleting stream with name : $streamName\n")
  }

}
