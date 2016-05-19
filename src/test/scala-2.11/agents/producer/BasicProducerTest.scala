package agents.producer

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.newcoordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.services.BasicStreamService
import com.datastax.driver.core.Cluster
import org.redisson.{Redisson, Config}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{RoundRobinPolicyCreator, LocalGeneratorCreator, CassandraHelper, RandomStringCreator}


class BasicProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringCreator.randomAlphaString(10)
  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  CassandraHelper.createDataTable(temporarySession, randomKeyspace)

  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory

  val config = new Config()
  config.useSingleServer().setAddress("localhost:6379")
  val redisson = Redisson.create(config)
  val coordinator = new Coordinator("some_path", redisson)

  val stringToArrayByteConverter = new StringToArrayByteConverter

  val cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)

  val stream = BasicStreamService.createStream(
    streamName = "test_stream",
    partitions = 3,
    ttl = 60 * 10,
    description = "unit_testing",
    metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
    dataStorage = storageFactory.getInstance(cassandraOptions),
    coordinator = coordinator)

  val agentSettings = new ProducerCoordinationSettings(
    agentAddress = s"localhost:8000",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5)

  val producerOptions = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 10,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0,1,2)),
    SingleElementInsert,
    LocalGeneratorCreator.getGen(),
    agentSettings,
    stringToArrayByteConverter)

  val producer = new BasicProducer("test_producer", stream, producerOptions)

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val txn: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.errorIfOpen)
    txn.checkpoint()
    txn.isInstanceOf[BasicProducerTransaction[_,_]] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 2)
    intercept[IllegalStateException] {
       val txn2 = producer.newTransaction(ProducerPolicies.errorIfOpen, 2)
    }
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 2)
    val txn2 = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 2)
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val txn = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 1)
    val txnRef = producer.getTransaction(1)
    val checkVal = txnRef.get == txn
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    val zkService = new ZkService("/unit", List(new InetSocketAddress("localhost",2181)), 7000)
    zkService.deleteRecursive("")
    zkService.close()
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
    redisson.shutdown()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}
