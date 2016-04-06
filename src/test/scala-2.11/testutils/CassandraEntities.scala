package testutils

import com.datastax.driver.core.Session


object CassandraEntities {

  /**
   * Keyspace creator
   * @param session session instance which will be used for keyspace creation
   * @param keyspace keyspace name
   * @return Unit
   */
  def createKeyspace(session: Session, keyspace : String) = session.execute(s"CREATE KEYSPACE $keyspace WITH replication = " +
                                                                  s" {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
                                                                  s" AND durable_writes = true")

  /**
   * Metadata tables creator
   * @param session session instance which will be used for tables creation
   * @param keyspace keyspace name
   * @return Unit
   */
  def createMetadataTables(session : Session, keyspace : String) = {
    
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
   * Cassandra data table creator
   * @param session session instance which will use for table creation
   * @param keyspace keyspace name
   * @return Unit
   */
  def createDataTable(session: Session, keyspace: String) = {

    session.execute(s"CREATE TABLE $keyspace.data_queue ( " +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"seq int, " +
      s"data blob, " +
      s"PRIMARY KEY ((stream, partition), transaction, seq))")
  }

  def dropDataTable(session: Session, keyspace : String) = {
    session.execute(s"DROP TABLE $keyspace.data_queue")
  }

  def dropMetadataTables(session: Session, keyspace : String) = {
    session.execute(s"DROP TABLE $keyspace.stream_commit_last")

    session.execute(s"DROP TABLE $keyspace.consumers")

    session.execute(s"DROP TABLE $keyspace.streams")

    session.execute(s"DROP TABLE $keyspace.commit_log")

    session.execute(s"DROP TABLE $keyspace.generators")
  }


  def clearTables(session: Session, keyspace : String) = {
    dropDataTable(session, keyspace)
    createDataTable(session, keyspace)
    dropMetadataTables(session, keyspace)
    createMetadataTables(session, keyspace)
  }
}
