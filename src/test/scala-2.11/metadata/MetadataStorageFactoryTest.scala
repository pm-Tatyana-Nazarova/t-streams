package metadata

import java.net.InetSocketAddress

import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{RandomStringGen, CassandraHelper}


class MetadataStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  def randomString: String = RandomStringGen.randomAlphaString(10)

  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()

    CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
    CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  }

  "MetadataStorageFactory.getInstance()" should "return MetadataStorage instance" in {

    val factory = new MetadataStorageFactory
    val instance = factory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)
    val checkVal = instance.isInstanceOf[MetadataStorage]
    checkVal shouldEqual true
  }

  "MetadataStorageFactory.closeFactory()" should "closes instance connections" in {

    val factory: MetadataStorageFactory = new MetadataStorageFactory
    val instance1 = factory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)
    val instance2 = factory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)
    factory.closeFactory()
    val checkVal = instance1.isClosed && instance2.isClosed
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    val newCluster = Cluster.builder().addContactPoint("localhost").build()
    val newSession: Session = newCluster.connect()
    newSession.execute(s"DROP KEYSPACE $randomKeyspace")
    newCluster.close()
    newSession.close()

    if (!temporarySession.isClosed)
      temporarySession.close()
    if(!temporaryCluster.isClosed)
      temporaryCluster.close()
  }
}
