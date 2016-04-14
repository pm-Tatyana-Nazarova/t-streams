package entities

import com.bwsw.tstreams.entities.CommitEntity
import com.datastax.driver.core.Cluster
import com.gilt.timeuuid.TimeUuid
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringCreator}


class RefreshEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringCreator.randomAlphaString(10)
  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  val connectedSession = temporaryCluster.connect(randomKeyspace)

  "After dropping metadata tables and creating them again commit entity" should "work" in {
    val commitEntity = new CommitEntity("commit_log", connectedSession)
    val stream = randomString
    val txn = TimeUuid()
    val partition = 10
    val totalCnt = 123
    val ttl = 3

    commitEntity.commit(stream, partition, txn, totalCnt, ttl)

    //refresh metadata
    CassandraHelper.dropMetadataTables(connectedSession, randomKeyspace)
    CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
    commitEntity.commit(stream, partition, txn, totalCnt, ttl)
  }
}
