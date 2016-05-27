package agents.both.group_commit

import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{ConsumerCoordinationSettings, BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.{ProducerCoordinationSettings, ProducerPolicies, BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.converter.{StringToArrayByteConverter, ArrayByteToStringConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{LocalGeneratorCreator, RoundRobinPolicyCreator, CassandraHelper, RandomStringCreator}


class GroupCommitTest extends FlatSpec with Matchers with BeforeAndAfterAll{
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
  val aerospikeInst = storageFactory.getInstance(aerospikeOptions)
  
  val metadataStorage = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)


  val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorage,
    dataStorage = aerospikeInst,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorage,
    dataStorage = aerospikeInst,
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
  var consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
  
  "Group commit" should "checkpoint all AgentsGroup state" in {
    val group = new CheckpointGroup()
    group.add("producer", producer)
    group.add("consumer", consumer)

    val txn = producer.newTransaction(ProducerPolicies.errorIfOpen)
    txn.send("info1")
    txn.checkpoint()

    //move consumer offsets
    val consumedTxn = consumer.getTransaction.get

    //open transaction without close
    producer.newTransaction(ProducerPolicies.errorIfOpen).send("info2")

    group.commit()

    consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
    //assert that the second transaction was closed and consumer offsets was moved
    consumer.getTransaction.get.getAll().head == "info2"
  }

  override def afterAll(): Unit = {
    val zkService = new ZkService("/unit", List(new InetSocketAddress("localhost",2181)), 7000)
    zkService.deleteRecursive("")
    zkService.close()
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}
