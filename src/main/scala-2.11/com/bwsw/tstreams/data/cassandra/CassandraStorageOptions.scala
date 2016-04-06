package com.bwsw.tstreams.data.cassandra

/**
 * Options for cassandra
 * @param cassandraHosts Cassandra hosts to connect
 * @param keyspace Cassandra keyspace to connect
 */
class CassandraStorageOptions(val cassandraHosts : List[String],
                              val keyspace : String){
  if (cassandraHosts.isEmpty)
    throw new IllegalArgumentException("cassandra hosts can't be empty")
  if (keyspace == null)
    throw new IllegalArgumentException("cassandra keyspace can't be null")
}
