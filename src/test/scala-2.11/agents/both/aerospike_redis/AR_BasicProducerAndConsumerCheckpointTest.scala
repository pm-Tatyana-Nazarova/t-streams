package agents.both.aerospike_redis

import java.net.InetSocketAddress
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{BasicConsumerTransaction, BasicConsumerOptions, BasicConsumer}
import com.bwsw.tstreams.agents.producer.{BasicProducerOptions, BasicProducer}
import com.bwsw.tstreams.converter.{StringToArrayByteConverter, ArrayByteToStringConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Session, Cluster}
import org.redisson.Config
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}



class AR_BasicProducerAndConsumerCheckpointTest  extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var cluster : Cluster = null
  var session: Session = null
  var producer : BasicProducer[String,Array[Byte]] = null
  var consumer : BasicConsumer[Array[Byte],String] = null
  var consumerOptions: BasicConsumerOptions[Array[Byte], String] = null
  var streamForConsumer: BasicStream[Array[Byte]] = null
  var metadataStorageFactory: MetadataStorageFactory = null
  var storageFactory: AerospikeStorageFactory = null
  var lockerFactoryForProducer: RedisLockerFactory = null
  var lockerFactoryForConsumer: RedisLockerFactory = null


  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    cluster = Cluster.builder().addContactPoint("localhost").build()
    session = cluster.connect()
    CassandraEntities.createKeyspace(session, randomKeyspace)
    CassandraEntities.createMetadataTables(session, randomKeyspace)
    CassandraEntities.createDataTable(session, randomKeyspace)

    //factories for storages creation
    metadataStorageFactory = new MetadataStorageFactory
    storageFactory = new AerospikeStorageFactory

    //converters
    val arrayByteToStringConverter = new ArrayByteToStringConverter
    val stringToArrayByteConverter = new StringToArrayByteConverter

    //aerospike storages
    val hosts = List(
      new Host("localhost",3000),
      new Host("localhost",3001),
      new Host("localhost",3002),
      new Host("localhost",3003))
    val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
    val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)
    val aerospikeInstForConsumer = storageFactory.getInstance(aerospikeOptions)

    //metadata storages
    val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)

    //locker factories
    val config = new Config()
    config.useSingleServer().setAddress("localhost:6379")
    lockerFactoryForProducer = new RedisLockerFactory("/some_path", config)
    lockerFactoryForConsumer = new RedisLockerFactory("/some_path", config)

    //streams
    val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorageInstForProducer,
      dataStorage = aerospikeInstForProducer,
      lockService = lockerFactoryForProducer,
      ttl = 60 * 60 * 24,
      description = "some_description")

    streamForConsumer = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorageInstForConsumer,
      dataStorage = aerospikeInstForConsumer,
      lockService = lockerFactoryForConsumer,
      ttl = 60 * 60 * 24,
      description = "some_description")

    //options
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
      stringToArrayByteConverter)

    consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      PolicyRepository.getRoundRobinPolicy(streamForConsumer, List(0,1,2)),
      Oldest,
      useLastOffset = false)

    //agents
    producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
    consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
  }


  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val dataToSend = (for (i <- 0 until 10) yield randomString).sorted
    val txnNum = 20

    (0 until txnNum) foreach { _ =>
      val txn = producer.newTransaction(false)
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

    //reinitialization (should begin read from latest checkpoint)
    consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)

    (0 until secondPart) foreach { _ =>
      val txn: BasicConsumerTransaction[Array[Byte], String] = consumer.getTransaction.get
      val data = txn.getAll().sorted
      checkVal &= data == dataToSend
    }

    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    lockerFactoryForConsumer.closeFactory()
    lockerFactoryForProducer.closeFactory()
  }
}
