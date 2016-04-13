package com.bwsw.tstreams_benchmarks

import java.net.InetSocketAddress
import com.bwsw.tstreams_benchmarks.utils.CassandraHelper
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
  def initMetadata(cassandraHosts : List[InetSocketAddress], keyspace : String) = {
    val builder = Cluster.builder()
    cassandraHosts.foreach(x=>builder.addContactPointsWithPorts(x))
    val cluster = builder.build()
    val session = cluster.connect()
    CassandraHelper.createTables(session, keyspace)
    cluster.close()
    session.close()
  }
}
