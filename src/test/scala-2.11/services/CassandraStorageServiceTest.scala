package services

import com.bwsw.tstreams.services.{CassandraStorageService, CassandraStrategies}
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{RandomStringGen, CassandraHelper}

import scala.collection.mutable.ListBuffer


class CassandraStorageServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var maybeCreatedKeyspaces = ListBuffer[String]()

  "MetadataStorageService.createKeyspace() and MetadataStorageService.dropKeyspace()" should
    "create keyspace and drop it in cassandra" in {

    val randomKeyspace = randomString
    maybeCreatedKeyspaces+=randomKeyspace

    val cluster: Cluster = Cluster.builder().addContactPoint("localhost").build()

    CassandraStorageService.createKeyspace(List("localhost"),
      randomKeyspace,
      CassandraStrategies.SimpleStrategy)

    CassandraStorageService.dropKeyspace(List("localhost"),
      randomKeyspace)

    val maybeNull = cluster.getMetadata.getKeyspace(randomKeyspace)
    cluster.close()

    maybeNull shouldEqual null
  }

  "MetadataStorageService.createKeyspace()" should
    "create keyspace in cassandra" in {

    val randomKeyspace = randomString
    maybeCreatedKeyspaces += randomKeyspace

    val cluster: Cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()

    //testing method
    CassandraStorageService.createKeyspace(List("localhost"),
      randomKeyspace,
      CassandraStrategies.SimpleStrategy)

    val maybeNull = cluster.getMetadata.getKeyspace(randomKeyspace)

    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()

    maybeNull should not be null
  }

  "MetadataStorageService.dropKeyspace()" should
    "drop keyspace in cassandra which" in {

    val randomKeyspace = randomString
    maybeCreatedKeyspaces += randomKeyspace

    val cluster: Cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()

    CassandraHelper.createKeyspace(session, randomKeyspace)

    //testing method
    CassandraStorageService.dropKeyspace(List("localhost"),
      randomKeyspace)

    val maybeNull = cluster.getMetadata.getKeyspace(randomKeyspace)

    session.close()
    cluster.close()

    maybeNull shouldEqual null
  }

  override def afterAll(): Unit = {
    val cluster: Cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    val metadata = cluster.getMetadata
    maybeCreatedKeyspaces.foreach{ x=>
        if (metadata.getKeyspace(x) != null)
          session.execute(s"DROP KEYSPACE $x")
    }
    cluster.close()
    session.close()
  }
}
