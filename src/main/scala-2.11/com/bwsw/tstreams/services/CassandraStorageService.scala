package com.bwsw.tstreams.services

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory


/**
 * Service for metadata storage. Include static methods for cluster initialization,
 * creating new keyspace, dropping keyspace
 */
object CassandraStorageService {

  /**
   * MetadataStorageServiceLogger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Creates keyspace for Metadata.
   * @param cassandraHosts Ring hosts to join
   * @param keyspace Keyspace to create
   * @param replicationStrategy One of supported C* strategies (enum CassandraStrategies.CassandraStrategies)
   * @param replicationFactor How to replicate data. Default = 1
   */
  def createKeyspace(cassandraHosts: List[String],
                     keyspace: String,
                     replicationStrategy: CassandraStrategies.CassandraStrategies,
                     replicationFactor: Int = 1) = {
    logger.info(s"start creation keyspace:$keyspace\n")

    val cluster = getCluster(cassandraHosts)
    val session: Session = cluster.connect()

    logger.debug(s"start executing creation statement for keyspace:{$keyspace}\n")
    session.execute(s"CREATE KEYSPACE $keyspace WITH replication = " +
      s" {'class': '$replicationStrategy', 'replication_factor': '$replicationFactor'} " +
      s" AND durable_writes = true")

    session.close()
    cluster.close()

    logger.info(s"finished creation keyspace:$keyspace\n")
  }

  /**
   * Internal method to create cluster without session
   * @param hosts Hosts connect to
   * @return Cluster connected to hosts
   */
  private def getCluster(hosts: List[String]): Cluster = {
    logger.info(s"start creating cluster for hosts : {${hosts.mkString(",")}\n")
    val builder: Builder = Cluster.builder()
    hosts.foreach(x => builder.addContactPoint(x))
    val cluster = builder.build()
    logger.info(s"finished creating cluster for hosts : {${hosts.mkString(",")}\n")
    cluster
  }

  /**
   * Drops keyspace for Metadata.
   * @param cassandraHosts Ring hosts to join
   * @param keyspace Keyspace to drop
   */
  def dropKeyspace(cassandraHosts: List[String],
                   keyspace: String) = {
    logger.info(s"start dropping keyspace:$keyspace\n")

    val cluster = getCluster(cassandraHosts)
    val session: Session = cluster.connect()

    logger.debug(s"start keyspace:$keyspace dropping\n")
    session.execute(s"DROP KEYSPACE $keyspace")

    session.close()
    cluster.close()

    logger.info(s"finished dropping keyspace:$keyspace\n")
  }

}

/**
 * Enumeration for accessing available network topologies
 */
object CassandraStrategies extends Enumeration {
  type CassandraStrategies = Value
  val SimpleStrategy = Value("SimpleStrategy")
  val NetworkTopologyStrategy = Value("NetworkTopologyStrategy")
}