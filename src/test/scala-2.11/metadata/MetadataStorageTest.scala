package metadata

import com.bwsw.tstreams.metadata.MetadataStorage
import com.datastax.driver.core.Cluster
import org.scalatest._
import testutils.{RandomStringCreator, CassandraHelper}


class MetadataStorageTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  def randomString: String = RandomStringCreator.randomAlphaString(10)

  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()

  CassandraHelper.createKeyspace(session,randomKeyspace)
  CassandraHelper.createMetadataTables(session,randomKeyspace)

  val connectedSession = cluster.connect(randomKeyspace)

  "MetadataStorage.init(), MetadataStorage.truncate() and MetadataStorage.remove()" should "create, truncate and remove metadata tables" in {

    val metadataStorage = new MetadataStorage(cluster, connectedSession, randomKeyspace)

    var checkIfOk = true

    try {
      //here we already have created tables for entitiesRepository
      metadataStorage.remove()
      metadataStorage.init()
      metadataStorage.truncate()
    }
    catch{
      case e : Exception => checkIfOk = false
    }
    checkIfOk shouldEqual true
  }

  override def afterAll() : Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    cluster.close()
    session.close()
    connectedSession.close()
  }
}