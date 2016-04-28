package com.bwsw.tstreams_benchmarks.utils

import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException

/**
 * Test util for creating C* entities
 */
object CassandraHelper {

  /**
   * Keyspace creator helper
    *
    * @param session Session instance which will be used for keyspace creation
   * @param keyspace Keyspace name
   */
  def createKeyspace(session: Session, keyspace : String) = session.execute(s"CREATE KEYSPACE $keyspace WITH replication = " +
                                                                  s" {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
                                                                  s" AND durable_writes = true")

  /**
   * Metadata tables creator helper
    *
    * @param session Session
   * @param keyspace Keyspace name
   */
  private def createMetadataTables(session : Session, keyspace : String) = {
    
    session.execute(s"CREATE TABLE $keyspace.stream_commit_last (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"PRIMARY KEY (stream, partition))")
    
    session.execute(s"CREATE TABLE $keyspace.consumers (" +
      s"name text, " +
      s"stream text, " +
      s"partition int, " +
      s"last_transaction timeuuid, " +
      s"PRIMARY KEY (name, stream, partition))")

    
    session.execute(s"CREATE TABLE $keyspace.streams (" +
      s"stream_name text PRIMARY KEY, " +
      s"partitions int," +
      s"ttl int, " +
      s"description text)")

    
    session.execute(s"CREATE TABLE $keyspace.commit_log (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"cnt int, " +
      s"PRIMARY KEY (stream, partition, transaction))")

    
    session.execute(s"CREATE TABLE $keyspace.generators (" +
      s"name text, " +
      s"time timeuuid, " +
      s"PRIMARY KEY (name))")
    
  }

  /**
    * Cassandra data table creator helper
    *
    * @param session Session
    * @param keyspace Keyspace name
    */
  private def createDataTable(session: Session, keyspace: String) = {

    session.execute(s"CREATE TABLE $keyspace.data_queue ( " +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"seq int, " +
      s"data blob, " +
      s"PRIMARY KEY ((stream, partition), transaction, seq))")
  }

  def createTables(session: Session, keyspace: String) = {
    dropKeyspace(session, keyspace)
    createKeyspace(session, keyspace)
    createMetadataTables(session, keyspace)
    createDataTable(session, keyspace)
  }

  /**
    * Cassandra storage keyspace dropper helper
    *
    * @param session Session
    * @param keyspace Keyspace name
    */
  private def dropKeyspace(session: Session, keyspace : String) = {
    try {
      session.execute(s"DROP KEYSPACE $keyspace")
    }
    catch {
      case e: InvalidConfigurationInQueryException => // ignore exception if keyspace doesn't exist
    }
  }
}
