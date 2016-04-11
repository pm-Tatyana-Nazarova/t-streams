package com.bwsw.tstreams.metadata

import java.net.InetSocketAddress

import com.bwsw.tstreams.entities._
import com.bwsw.tstreams.utils.LocalTimeTxnGenerator
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory


/**
 * Class of MetadataStorage
 * @param cluster Cluster instance for metadata storage
 * @param session Session instance for metadata storage
 * @param keyspace Keyspace for metadata storage
 */
class MetadataStorage(cluster: Cluster, session: Session, keyspace: String) {

  /**
   * MetadataStorage logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Stream entity instance
   */
  lazy val streamEntity = new StreamEntity("streams", session)

  /**
   * Commit entity instance
   */
  lazy val commitEntity = new CommitEntity("commit_log", session)

  /**
   * Commit entity instance for producer async txn update
   */
  lazy val producerCommitEntity = new CommitEntity("commit_log", session)

  /**
   * Consumer entity instance
   */
  lazy val consumerEntity = new ConsumerEntity("consumers", session)

  /**
   * @return Keyspace
   */
  def getKeyspace = keyspace

  /**
   * @return Closed this storage or not
   */
  def isClosed : Boolean = session.isClosed && cluster.isClosed

  /**
   * Closes MetadataStorage
   */
  def close() = {
    session.close()
    cluster.close()
  }

  /**
    * Removes MetadataStorage
    */
  def remove() = {
    logger.info("start removing MetadataStorage tables from cassandra\n")

    logger.debug("dropping stream_commit_last table\n")
    session.execute("DROP TABLE stream_commit_last")

    logger.debug("dropping consumers table\n")
    session.execute("DROP TABLE consumers")

    logger.debug("dropping streams table\n")
    session.execute("DROP TABLE streams")

    logger.debug("dropping commit_log table\n")
    session.execute("DROP TABLE commit_log")

    logger.debug("dropping generators table\n")
    session.execute("DROP TABLE generators")

    logger.info("finished removing MetadataStorage tables from cassandra\n")
  }

  /**
    * Initializes metadata (creates tables in C*)
    */
  def init() = {
    logger.info("start initializing MetadataStorage tables\n")

    logger.debug("start creating stream_commit_last table\n")
    session.execute(s"CREATE TABLE stream_commit_last (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"PRIMARY KEY (stream, partition))")

    logger.debug("start creating consumers table\n")
    session.execute(s"CREATE TABLE consumers (" +
      s"name text, " +
      s"stream text, " +
      s"partition int, " +
      s"last_transaction timeuuid, " +
      s"PRIMARY KEY (name, stream, partition))")

    logger.debug("start creating streams table\n")
    session.execute(s"CREATE TABLE streams (" +
      s"stream_name text PRIMARY KEY, " +
      s"partitions int, " +
      s"description text)")

    logger.debug("start creating commit log\n")
    session.execute(s"CREATE TABLE commit_log (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"cnt int, " +
      s"PRIMARY KEY (stream, partition, transaction))")

    logger.debug("start creating generators table\n")
    session.execute(s"CREATE TABLE generators (" +
      s"name text, " +
      s"time timeuuid, " +
      s"PRIMARY KEY (name))")

    logger.info("finished initializing MetadataStorage tables\n")
  }

  /**
    * Truncates data in metadata storage
    */
  def truncate() = {
    logger.info("start truncating MetadataStorage tables\n")

    logger.debug("removing data from stream_commit_last table\n")
    session.execute("TRUNCATE stream_commit_last")

    logger.debug("removing data from consumers table\n")
    session.execute("TRUNCATE consumers")

    logger.debug("removing data from streams table\n")
    session.execute("TRUNCATE streams")

    logger.debug("removing data from commit_log table\n")
    session.execute("TRUNCATE commit_log")

    logger.debug("removing data from generators table\n")
    session.execute("TRUNCATE generators")

    logger.info("finished truncating MetadataStorage tables\n")
  }

  /**
   * Validates that metadata created correctly
   * @return Correctness of created metadata
   */
  // TODO: implement validate and then delete deprecated annotation
  def validate() : Boolean = ???
}


/**
 * Factory for creating MetadataStorage instances
 */
class MetadataStorageFactory {
  /**
   * MetadataStorageFactory logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
    * Fabric method which returns new MetadataStorage
    * @param cassandraHosts List of hosts to connect in C* cluster
    * @param keyspace Keyspace to use for metadata storage
    * @return Instance of MetadataStorage
    */
  def getInstance(cassandraHosts : List[InetSocketAddress], keyspace : String): MetadataStorage = {
    logger.info("start MetadataStorage instance creation\n")

    val builder: Builder = Cluster.builder()
    cassandraHosts.foreach(x => builder.addContactPointsWithPorts(x))
    val cluster = builder.build()

    logger.debug(s"start connecting cluster to keyspace: {$keyspace}\n")
    val session: Session = cluster.connect(keyspace)

    val inst = new MetadataStorage(cluster, session, keyspace)
    instances += inst
    logger.info("finished MetadataStorage instance creation\n")
    inst
  }

  /**
   * Storage for all MetadataStorage instances
   */
  private var instances = ListBuffer[MetadataStorage]()

  /**
   * Closes all factory MetadataStorage instances
   */
  def closeFactory() = instances.foreach(_.close())

}

