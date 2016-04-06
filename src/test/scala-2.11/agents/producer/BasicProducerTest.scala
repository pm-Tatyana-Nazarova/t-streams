package agents.producer

import com.bwsw.tstreams.agents.producer.{BasicProducerTransaction, BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.lockservice.impl.{ZkServer, ZkLockerFactory}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}




class BasicProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var producer : BasicProducer[String,Array[Byte]] = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntities.createDataTable(temporarySession, randomKeyspace)

    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new CassandraStorageFactory
    val stringToArrayByteConverter = new StringToArrayByteConverter
    val lockService = new ZkLockerFactory(List(ZkServer("localhost", 2181)), "/some_path", 10)
    val cassandraOptions = new CassandraStorageOptions(List("localhost"), randomKeyspace)
    val stream = BasicStreamService.createStream(
      streamName = "test_stream",
      partitions = 3,
      ttl = 60 * 60 * 24,
      description = "unit_testing",
      metadataStorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace),
      dataStorage = storageFactory.getInstance(cassandraOptions),
      lockService = lockService)
    val options = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 10,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(stream, List(0,1,2)),
      stringToArrayByteConverter)

    producer = new BasicProducer("test_producer", stream, options)
  }

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val txn: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    txn.close()
    txn.isInstanceOf[BasicProducerTransaction[_,_]] shouldEqual true
  }

  "BasicProducer.newTransaction(false)" should "throw exception if previous transaction not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    intercept[IllegalStateException] {
       val txn2 = producer.newTransaction(false)
    }
    txn1.close()
  }

  "BasicProducer.newTransaction(true)" should "not throw exception if previous transaction not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    val txn2 = producer.newTransaction(true)
  }


  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
