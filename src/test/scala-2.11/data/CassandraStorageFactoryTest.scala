package data

import java.net.InetSocketAddress
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory, CassandraStorage}
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringGen}


class CassandraStorageFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)

  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()

  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createDataTable(temporarySession, randomKeyspace)
  val cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)

  "CassandraStorageFactory.getInstance()" should "return CassandraStorage instance" in {

    val factory = new CassandraStorageFactory
    val instance = factory.getInstance(cassandraOptions)
    val checkVal = instance.isInstanceOf[CassandraStorage]
    factory.closeFactory()

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
