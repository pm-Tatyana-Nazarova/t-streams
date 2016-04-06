package com.bwsw.tstreams.data.aerospike

import com.bwsw.tstreams.data.IStorage
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * Factory for creating Aerospike storage instances
 */
class AerospikeStorageFactory{
  /**
   * Instances of all Aerospike storage
   */
  private var instances = ListBuffer[IStorage[Array[Byte]]]()

  /**
   * AerospikeStorage logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * @param aerospikeOptions Options of aerospike client
   * @return Instance of CassandraStorage
   */
  def getInstance(aerospikeOptions: AerospikeStorageOptions) : IStorage[Array[Byte]] = {
    logger.info(s"start AerospikeStorage instance creation\n")
    val inst = new AerospikeStorage(aerospikeOptions)
    logger.info(s"finished AerospikeStorage instance creation\n")
    instances += inst
    inst
  }

  /**
   * Close all factory storage instances
   */
  def closeFactory() : Unit = {
    logger.info("start closing Aerospike Storage Factory")
    instances.foreach(_.close())
    logger.info("finished closing Aerospike Storage Factory")
  }
}
