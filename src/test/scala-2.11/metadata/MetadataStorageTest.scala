package metadata

import com.bwsw.tstreams.metadata.MetadataStorage
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest._
import testutils.{RandomStringGen, CassandraHelper}


class MetadataStorageTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  def randomString: String = RandomStringGen.randomAlphaString(10)

  var randomKeyspace : String = null
  var cluster: Cluster = null
  var session: Session = null
  var connectedSession : Session = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    cluster = Cluster.builder().addContactPoint("localhost").build()
    session = cluster.connect()

    CassandraHelper.createKeyspace(session,randomKeyspace)
    CassandraHelper.createMetadataTables(session,randomKeyspace)

    connectedSession = cluster.connect(randomKeyspace)
  }

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
    val newCluster = Cluster.builder().addContactPoint("localhost").build()
    val newSession: Session = newCluster.connect()
    newSession.execute(s"DROP KEYSPACE $randomKeyspace")
    newCluster.close()
    newSession.close()

    if (!cluster.isClosed)
      cluster.close()
    if (!session.isClosed)
      session.close()
    if (!connectedSession.isClosed)
      connectedSession.close()
  }

}