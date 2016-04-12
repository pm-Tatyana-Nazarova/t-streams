package com.bwsw.tstreams.data.cassandra

import com.bwsw.tstreams.data.IStorage
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


/**
 * Factory for creating cassandra storage instances
 */
class CassandraStorageFactory {
  /**
   * Instances of all CassandraStorage
   */
  private var instances = ListBuffer[IStorage[Array[Byte]]]()

  /**
   * CassandraStorageFactory logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   *
   * @param cassandraStorageOptions Cassandra client options
   * @return Instance of CassandraStorage
   */
  def getInstance(cassandraStorageOptions: CassandraStorageOptions) : CassandraStorage = {
    logger.info(s"start CassandraStorage instance creation with keyspace : {${cassandraStorageOptions.keyspace}}\n")

    val builder: Builder = Cluster.builder()

    cassandraStorageOptions.cassandraHosts.foreach(x => builder.addContactPointsWithPorts(x))

    val cluster = builder.build()

    logger.debug(s"start connecting cluster to keyspace: {${cassandraStorageOptions.keyspace}}\n")
    val session: Session = cluster.connect(cassandraStorageOptions.keyspace)

    logger.debug(s"start creating CassandraStorage with keyspace {${cassandraStorageOptions.keyspace}}:\n")
    val inst = new CassandraStorage(cluster, session, cassandraStorageOptions.keyspace)

    instances += inst

    logger.info(s"finished CassandraStorage instance creation with keyspace : {${cassandraStorageOptions.keyspace}}\n")
    inst
  }


  /**
   * Close all factory storage instances
   */
  def closeFactory() : Unit = instances.foreach(_.close())
}
