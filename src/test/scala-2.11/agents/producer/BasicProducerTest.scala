package agents.producer

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.producer.{BasicProducerTransaction, BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.lockservice.impl.ZkLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringGen}


class BasicProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)

  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  CassandraHelper.createDataTable(temporarySession, randomKeyspace)

  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory
  val lockService = new ZkLockerFactory(List(new InetSocketAddress("localhost", 2181)), "/some_path", 10)

  val stringToArrayByteConverter = new StringToArrayByteConverter
  val cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)
  val stream = BasicStreamService.createStream(
    streamName = "test_stream",
    partitions = 3,
    ttl = 60 * 10,
    description = "unit_testing",
    metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
    dataStorage = storageFactory.getInstance(cassandraOptions),
    lockService = lockService)
  val options = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 10,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    PolicyRepository.getRoundRobinPolicy(stream, List(0,1,2)),
    stringToArrayByteConverter)

  val producer = new BasicProducer("test_producer", stream, options)


  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val txn: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    txn.close()
    txn.isInstanceOf[BasicProducerTransaction[_,_]] shouldEqual true
  }

  "BasicProducer.newTransaction(false)" should "throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    intercept[IllegalStateException] {
       val txn2 = producer.newTransaction(false)
    }
    txn1.close()
  }

  "BasicProducer.newTransaction(true)" should "not throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    val txn2 = producer.newTransaction(true)
  }


  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
    lockService.closeFactory()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}
