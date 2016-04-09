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
import testutils.{CassandraHelper, RandomStringGen}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class AR_BasicProducerAndConsumerSimpleTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var cluster : Cluster = null
  var session: Session = null
  var producer : BasicProducer[String,Array[Byte]] = null
  var consumer : BasicConsumer[Array[Byte],String] = null
  var metadataStorageFactory: MetadataStorageFactory = null
  var storageFactory: AerospikeStorageFactory = null
  var lockerFactoryForProducer: RedisLockerFactory = null
  var lockerFactoryForConsumer: RedisLockerFactory = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    cluster = Cluster.builder().addContactPoint("localhost").build()
    session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)

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

    val streamForConsumer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
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

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
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

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it with getAll method" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalDataInTxn = 10
    val producerTransaction = producer.newTransaction(false)
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + randomString).sorted
    sendData.foreach{ x=>
      producerTransaction.send(x)
    }
    producerTransaction.close()
    val txnOpt = consumer.getTransaction
    val txn = txnOpt.get

    var checkVal = txn.getAll().sorted == sendData

    //assert that is nothing to read
    for(i <- 0 until consumer.stream.getPartitions)
      checkVal &= consumer.getTransaction.isEmpty

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it using iterator" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalDataInTxn = 10
    val producerTransaction = producer.newTransaction(false)
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + randomString).sorted
    sendData.foreach{ x=>
      producerTransaction.send(x)
    }
    producerTransaction.close()
    val txnOpt = consumer.getTransaction
    assert(txnOpt.isDefined)
    val txn = txnOpt.get
    var dataToAssert = ListBuffer[String]()
    while(txn.hasNext()){
      dataToAssert += txn.next()
    }

    var checkVal = true

    //assert that is nothing to read
    for(i <- 0 until consumer.stream.getPartitions)
      checkVal &= consumer.getTransaction.isEmpty

    checkVal &= dataToAssert.toList.sorted == sendData

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate some set of transactions, consumer - retrieve them all" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalTxn = 10
    val totalDataInTxn = 10000
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + randomString).sorted

    (0 until totalTxn).foreach { _=>
        val producerTransaction = producer.newTransaction(false)
        sendData.foreach{ x=>
          producerTransaction.send(x)
        }
        producerTransaction.close()
    }

    var checkVal = true

    (0 until totalTxn).foreach { _=>
        val txn = consumer.getTransaction
        checkVal &= txn.nonEmpty
        checkVal &= txn.get.getAll().sorted == sendData
    }

    //assert that is nothing to read
    for(i <- 0 until consumer.stream.getPartitions)
      checkVal &= consumer.getTransaction.isEmpty

    checkVal shouldBe true
  }

  "producer, consumer" should "producer - generate transaction async, consumer retrieve it async" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val timeoutForWaiting = 120
    val totalDataInTxn = 10
    val sendData = (for (part <- 0 until totalDataInTxn) yield "data_part_" + part).sorted

    val producerThread = new Thread(new Runnable {
      def run() {
        val txn = producer.newTransaction(false)
        sendData.foreach{ x=>
          txn.send(x)
          Thread.sleep(1000)
        }
        txn.close()
      }
    })

    var checkVal = true

    val consumerThread = new Thread(new Runnable {
      def run() {
        breakable{while(true) {
          val consumedTxn: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getTransaction
          if (consumedTxn.isDefined) {
            checkVal &= consumedTxn.get.getAll().sorted == sendData
            break()
          }
          Thread.sleep(1000)
        }}
      }
    })

    producerThread.start()
    consumerThread.start()
    producerThread.join(timeoutForWaiting*1000)
    consumerThread.join(timeoutForWaiting*1000)

    checkVal &= !producerThread.isAlive
    checkVal &= !consumerThread.isAlive

    //assert that is nothing to read
    for(i <- 0 until consumer.stream.getPartitions)
      checkVal &= consumer.getTransaction.isEmpty

    checkVal shouldEqual true
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

