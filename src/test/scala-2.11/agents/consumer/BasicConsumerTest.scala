package agents.consumer

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.consumer.{BasicConsumerTransaction, BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.converter.ArrayByteToStringConverter
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory, CassandraStorage}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringGen}


class BasicConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)

  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)
  CassandraHelper.createDataTable(session, randomKeyspace)

  //factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory

  //converter
  val arrayByteToStringConverter = new ArrayByteToStringConverter

  //factory instances
  val mstorage: MetadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)
  val cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)
  val storage: CassandraStorage = storageFactory.getInstance(cassandraOptions)

  val stream: BasicStream[Array[Byte]] = BasicStreamService.createStream("test_stream", 3, 60*60*24, "unit_testing", mstorage, storage, null)

  val options = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    PolicyRepository.getRoundRobinPolicy(stream, List(0,1,2)),
    Oldest,
    useLastOffset = false)

  val consumer = new BasicConsumer("test_consumer", stream, options)

  "consumer.getTransaction()" should "return None because no data was sent" in {
    val txn: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getTransaction
    txn shouldEqual None
  }

  override def afterAll(): Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}
