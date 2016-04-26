package com.bwsw.tstreams.data.aerospike

import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.{Host, AerospikeClient}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * Factory for creating Aerospike storage instances
 */
class AerospikeStorageFactory{

  /**
   * Map for memorize clients which are already created
   */
  private val aerospikeClients = scala.collection.mutable.Map[(List[Host], ClientPolicy), AerospikeClient]()

  /**
   * AerospikeStorage logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * @param aerospikeOptions Options of aerospike client
   * @return Instance of CassandraStorage
   */
  def getInstance(aerospikeOptions: AerospikeStorageOptions) : AerospikeStorage = {
    logger.info(s"start AerospikeStorage instance creation\n")

    val client = {
      if (aerospikeClients.contains((aerospikeOptions.hosts, aerospikeOptions.clientPolicy))) {
        aerospikeClients((aerospikeOptions.hosts, aerospikeOptions.clientPolicy))
      }
      else{
        val client = new AerospikeClient(aerospikeOptions.clientPolicy,aerospikeOptions.hosts:_*)
        aerospikeClients((aerospikeOptions.hosts, aerospikeOptions.clientPolicy)) = client
        client
      }
    }

    logger.info(s"finished AerospikeStorage instance creation\n")
    new AerospikeStorage(client, aerospikeOptions)
  }

  /**
   * Close all factory storage instances
   */
  def closeFactory() : Unit = {
    logger.info("start closing Aerospike Storage Factory")
    aerospikeClients.foreach(x=>x._2.close())
    aerospikeClients.clear()
    logger.info("finished closing Aerospike Storage Factory")
  }
}
