package agents.consumer

import com.bwsw.tstreams.agents.consumer.{BasicConsumerTransaction, BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.converter.ArrayByteToStringConverter
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory, CassandraStorage}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}


class BasicConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var consumer : BasicConsumer[Array[Byte],String] = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntities.createDataTable(temporarySession, randomKeyspace)
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new CassandraStorageFactory
    val arrayByteToStringConverter = new ArrayByteToStringConverter
    val mstorage: MetadataStorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace)
    val cassandraOptions = new CassandraStorageOptions(List("localhost"), randomKeyspace)
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

    consumer = new BasicConsumer("test_consumer", stream, options)
  }

  "consumer.getTransaction()" should "return None" in {
    val txn: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getTransaction
    txn shouldEqual None
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
