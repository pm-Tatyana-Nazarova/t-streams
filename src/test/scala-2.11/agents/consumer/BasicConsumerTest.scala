package agents.consumer

import java.net.InetSocketAddress
import java.util.UUID
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{ConsumerCoordinationSettings, BasicConsumerTransaction, BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.producer.{ProducerCoordinationSettings, ProducerPolicies, BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.converter.{StringToArrayByteConverter, ArrayByteToStringConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.entities.CommitEntity
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{LocalGeneratorCreator, RoundRobinPolicyCreator, CassandraHelper, RandomStringCreator}


class BasicConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringCreator.randomAlphaString(10)
  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)

  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory

  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  val hosts = List(
    new Host("localhost",3000),
    new Host("localhost",3001),
    new Host("localhost",3002),
    new Host("localhost",3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)
  val aerospikeInstForConsumer = storageFactory.getInstance(aerospikeOptions)

  val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)
  val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)

  val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForProducer,
    dataStorage = aerospikeInstForProducer,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = aerospikeInstForConsumer,
    ttl = 60 * 10,
    description = "some_description")

  val agentSettings = new ProducerCoordinationSettings(
    agentAddress = s"localhost:8000",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5)

  val producerOptions = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 6,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
    SingleElementInsert,
    LocalGeneratorCreator.getGen(),
    agentSettings,
    stringToArrayByteConverter)

  val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0,1,2)),
    new ConsumerCoordinationSettings("localhost:8588", "/unit", List(new InetSocketAddress("localhost",2181)), 7000),
    Oldest,
    LocalGeneratorCreator.getGen(),
    useLastOffset = true)

  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  val consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
  val connectedSession = cluster.connect(randomKeyspace)

  "consumer.getTransaction" should "return None if nothing was sent" in {
    val txn = consumer.getTransaction
    txn.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val totalDataInTxn = 10
    val data = (for (i <- 0 until totalDataInTxn) yield randomString).toList.sorted
    val txn = producer.newTransaction(ProducerPolicies.errorIfOpen, 1)
    val txnUuid = txn.getTxnUUID
    data.foreach(x=>txn.send(x))
    txn.checkpoint()

    var checkVal = true

    val consumedTxn = consumer.getTransactionById(1, txnUuid).get
    checkVal = consumedTxn.getPartition == txn.getPartition
    checkVal = consumedTxn.getTxnUUID == txnUuid
    checkVal = consumedTxn.getAll().sorted == data

    checkVal shouldEqual true
  }

  "consumer.getTransaction" should "return sent transaction" in {
    val txn = consumer.getTransaction
    txn.isDefined shouldEqual true
  }

  "consumer.getLastTransaction" should "return last closed transaction" in {
    val commitEntity = new CommitEntity("commit_log", connectedSession)
    val txns = for (i <- 0 until 500) yield UUIDs.timeBased()

    val txn : UUID = txns.head

    commitEntity.commit("test_stream", 1, txns.head, 1, 120)

    txns.drop(1) foreach { x =>
      commitEntity.commit("test_stream", 1, x, -1, 120)
    }

    val retrievedTxnOpt: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getLastTransaction(partition = 1)
    val retrievedTxn = retrievedTxnOpt.get
    retrievedTxn.getTxnUUID shouldEqual txn
  }

  override def afterAll(): Unit = {
    val zkService = new ZkService("/unit", List(new InetSocketAddress("localhost",2181)), 7000)
    zkService.deleteRecursive("")
    zkService.close()
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    connectedSession.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}