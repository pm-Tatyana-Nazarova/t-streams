package com.bwsw.benchmarks

import com.bwsw.utils.CassandraEntitiesCreator
import com.datastax.driver.core.Cluster

/**
 * Metadata helper creator
 */
trait MetadataCreator {
  /**
   *
   * @param cassandraHosts C* hosts to connect
   * @param keyspace keyspace to create
   */
  def initMetadata(cassandraHosts : List[String], keyspace : String) = {
    val builder = Cluster.builder()
    cassandraHosts.foreach(x=>builder.addContactPoint(x))
    val cluster = builder.build()
    val session = cluster.connect()
    CassandraEntitiesCreator.createKeyspace(session, keyspace)
    CassandraEntitiesCreator.createMetadataTables(session, keyspace)
    cluster.close()
    session.close()
  }
}
