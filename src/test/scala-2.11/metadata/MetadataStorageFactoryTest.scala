package metadata

import java.net.InetSocketAddress
import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{RandomStringGen, CassandraHelper}


class MetadataStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  def randomString: String = RandomStringGen.randomAlphaString(10)

  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()

  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)

  "MetadataStorageFactory.getInstance()" should "return MetadataStorage instance" in {
    val factory = new MetadataStorageFactory
    val instance = factory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)
    val checkVal = instance.isInstanceOf[MetadataStorage]
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
