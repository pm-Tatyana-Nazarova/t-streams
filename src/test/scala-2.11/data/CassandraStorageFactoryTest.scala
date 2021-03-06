package data

import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory, CassandraStorage}
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}


class CassandraStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll{
    def randomString: String = RandomStringGen.randomAlphaString(10)

    var randomKeyspace : String = null
    var temporaryCluster : Cluster = null
    var temporarySession: Session = null
    var cassandraOptions : CassandraStorageOptions = null

    override def beforeAll(): Unit = {
      randomKeyspace = randomString
      temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
      temporarySession = temporaryCluster.connect()

      CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
      CassandraEntities.createDataTable(temporarySession, randomKeyspace)
      cassandraOptions = new CassandraStorageOptions(List("localhost"), randomKeyspace)
    }

    "CassandraStorageFactory.getInstance()" should "return CassandraStorage instance" in {

      val factory = new CassandraStorageFactory
      val instance = factory.getInstance(cassandraOptions)
      val checkVal = instance.isInstanceOf[CassandraStorage]
      checkVal shouldEqual true
    }

    "CassandraStorageFactory.closeFactory()" should "close instances connections" in {

      val factory: CassandraStorageFactory = new CassandraStorageFactory
      val instance1 = factory.getInstance(cassandraOptions)
      val instance2 = factory.getInstance(cassandraOptions)
      factory.closeFactory()
      val checkVal = instance1.isClosed && instance2.isClosed

      checkVal shouldEqual true
    }

    override def afterAll(): Unit = {
      temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
      temporarySession.close()
      temporaryCluster.close()
    }
}
