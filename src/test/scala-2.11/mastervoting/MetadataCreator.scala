package mastervoting

import com.datastax.driver.core.Cluster
import testutils.CassandraHelper

object MetadataCreator {
  def main(args: Array[String]) {
    if (args.isEmpty)
      throw new IllegalArgumentException("specify keyspace")
    val randomKeyspace = args(0)
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)
  }
}
