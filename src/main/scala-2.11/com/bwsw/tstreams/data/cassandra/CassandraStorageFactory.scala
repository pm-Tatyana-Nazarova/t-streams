package com.bwsw.tstreams.data.cassandra

import java.net.InetSocketAddress
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory


/**
 * Factory for creating cassandra storage instances
 */
class CassandraStorageFactory {
  /**
   * CassandraStorageFactory logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Map for memorize clusters which are already created
   */
  private val clusterMap = scala.collection.mutable.Map[List[InetSocketAddress], Cluster]()

  /**
   * Map for memorize sessions which are already created
   */
  private val sessionMap = scala.collection.mutable.Map[(List[InetSocketAddress], String), Session]()

  /**
   *
   * @param cassandraStorageOptions Cassandra client options
   * @return Instance of CassandraStorage
   */
  def getInstance(cassandraStorageOptions: CassandraStorageOptions) : CassandraStorage = {
    logger.info(s"start CassandraStorage instance creation with keyspace : {${cassandraStorageOptions.keyspace}}\n")

    val sortedHosts = cassandraStorageOptions.cassandraHosts.map(x=>(x,x.hashCode())).sortBy(_._2).map(x=>x._1)

    val cluster = {
      if (clusterMap.contains(sortedHosts))
        clusterMap(sortedHosts)
      else{
        val builder: Builder = Cluster.builder()
        cassandraStorageOptions.cassandraHosts.foreach(x => builder.addContactPointsWithPorts(x))
        val cluster = builder.build()
        clusterMap(sortedHosts) = cluster
        cluster
      }
    }

    val session = {
      if (sessionMap.contains((sortedHosts,cassandraStorageOptions.keyspace)))
        sessionMap((sortedHosts,cassandraStorageOptions.keyspace))
      else{
        val session: Session = cluster.connect(cassandraStorageOptions.keyspace)
        sessionMap((sortedHosts, cassandraStorageOptions.keyspace)) = session
        session
      }
    }

    logger.info(s"finished CassandraStorage instance creation with keyspace : {${cassandraStorageOptions.keyspace}}\n")
    new CassandraStorage(cluster, session, cassandraStorageOptions.keyspace)
  }

  /**
   * Close all factory storage instances
   */
  def closeFactory() : Unit = {
    clusterMap.foreach{x=>x._2.close()}
    sessionMap.foreach{x=>x._2.close()}
    clusterMap.clear()
    sessionMap.clear()
  }
}
