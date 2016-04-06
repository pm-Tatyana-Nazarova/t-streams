package metadata

import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{RandomStringGen, CassandraEntities}


class MetadataStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  def randomString: String = RandomStringGen.randomAlphaString(10)

  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()

    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
  }

  "MetadataStorageFactory.getInstance()" should "return MetadataStorage instance" in {

    val factory = new MetadataStorageFactory
    val instance = factory.getInstance(List("localhost"), randomKeyspace)
    val checkVal = instance.isInstanceOf[MetadataStorage]
    checkVal shouldEqual true
  }

  "MetadataStorageFactory.closeFactory()" should "closes instance connections" in {

    val factory: MetadataStorageFactory = new MetadataStorageFactory
    val instance1 = factory.getInstance(List("localhost"), randomKeyspace)
    val instance2 = factory.getInstance(List("localhost"), randomKeyspace)
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
