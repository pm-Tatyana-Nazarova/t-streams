package agents.both.batch_insert.cassandra

import java.net.InetSocketAddress
import agents.both.batch_insert.BatchSizeTestVal
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions, BasicConsumerTransaction}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerPolicies}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.redisson.{Config, Redisson}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{CassandraHelper, LocalGeneratorCreator, RandomStringCreator, RoundRobinPolicyCreator}


class CBasicProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with BatchSizeTestVal{
  //creating keyspace, metadata, data
  def randomString: String = RandomStringCreator.randomAlphaString(10)
  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)
  CassandraHelper.createDataTable(session, randomKeyspace)

  //metadata/data factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory

  //converters to convert usertype->storagetype; storagetype->usertype
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  //cassandra storage instances
  val cassandraStorageOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)
  val cassandraInstForProducer = storageFactory.getInstance(cassandraStorageOptions)
  val cassandraInstForConsumer = storageFactory.getInstance(cassandraStorageOptions)

  //metadata storage instances
  val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)
  val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)

  //coordinator for coordinating producer/consumer
  val config = new Config()
  config.useSingleServer().setAddress("localhost:6379")
  val redissonClient = Redisson.create(config)
  val coordinator = new Coordinator("some_path", redissonClient)

  //stream instances for producer/consumer
  val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForProducer,
    dataStorage = cassandraInstForProducer,
    coordinator = coordinator,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = cassandraInstForConsumer,
    coordinator = coordinator,
    ttl = 60 * 10,
    description = "some_description")

  //producer/consumer options
  val producerOptions = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 6,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
    BatchInsert(batchSizeVal),
    LocalGeneratorCreator.getGen(),
    stringToArrayByteConverter)

  val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0,1,2)),
    Oldest,
    LocalGeneratorCreator.getGen(),
    useLastOffset = true)

  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  var consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)


  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val dataToSend = (for (i <- 0 until 10) yield randomString).sorted
    val txnNum = 20

    (0 until txnNum) foreach { _ =>
      val txn = producer.newTransaction(ProducerPolicies.errorIfOpen)
      dataToSend foreach { part =>
        txn.send(part)
      }
      txn.close()
    }

    val firstPart = txnNum/3
    val secondPart = txnNum - firstPart

    var checkVal = true

    (0 until firstPart) foreach { _ =>
      val txn: BasicConsumerTransaction[Array[Byte], String] = consumer.getTransaction.get
      val data = txn.getAll().sorted
      consumer.checkpoint()
      checkVal &= data == dataToSend
    }

    //reinitialization (should begin read from the latest checkpoint)
    consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)

    (0 until secondPart) foreach { _ =>
      val txn: BasicConsumerTransaction[Array[Byte], String] = consumer.getTransaction.get
      val data = txn.getAll().sorted
      checkVal &= data == dataToSend
    }

    //assert that is nothing to read
    (0 until streamForConsumer.getPartitions) foreach { _=>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    redissonClient.shutdown()
  }
}
