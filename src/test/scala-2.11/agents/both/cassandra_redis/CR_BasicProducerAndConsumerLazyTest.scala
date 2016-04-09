package agents.both.cassandra_redis

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Cluster, Session}
import org.redisson.Config
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{CassandraHelper, RandomStringGen}
import scala.util.control.Breaks._


class CR_BasicProducerAndConsumerLazyTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var cluster : Cluster = null
  var session: Session = null
  var producer1 : BasicProducer[String,Array[Byte]] = null
  var producer2 : BasicProducer[String,Array[Byte]] = null
  var consumer : BasicConsumer[Array[Byte],String] = null
  var metadataStorageFactory: MetadataStorageFactory = null
  var storageFactory: CassandraStorageFactory = null
  var lockerFactoryForProducer1 : RedisLockerFactory = null
  var lockerFactoryForProducer2: RedisLockerFactory = null
  var lockerFactoryForConsumer: RedisLockerFactory = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    cluster = Cluster.builder().addContactPoint("localhost").build()
    session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)
    CassandraHelper.createDataTable(session, randomKeyspace)

    //factories for storages creation
    metadataStorageFactory = new MetadataStorageFactory
    storageFactory = new CassandraStorageFactory

    //converters
    val arrayByteToStringConverter = new ArrayByteToStringConverter
    val stringToArrayByteConverter = new StringToArrayByteConverter

    //cassandra storages
    val cassandraStorageOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)
    val cassandraInstForProducer1 = storageFactory.getInstance(cassandraStorageOptions)
    val cassandraInstForProducer2 = storageFactory.getInstance(cassandraStorageOptions)
    val cassandraInstForConsumer = storageFactory.getInstance(cassandraStorageOptions)

    //metadata storages
    val metadataStorageInstForProducer1 = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val metadataStorageInstForProducer2 = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)

    //locker factories
    val config = new Config()
    config.useSingleServer().setAddress("localhost:6379")
    lockerFactoryForProducer1 = new RedisLockerFactory("/some_path", config)
    lockerFactoryForProducer2 = new RedisLockerFactory("/some_path", config)
    lockerFactoryForConsumer = new RedisLockerFactory("/some_path", config)

    //streams
    val streamForProducer1: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorageInstForProducer1,
      dataStorage = cassandraInstForProducer1,
      lockService = lockerFactoryForProducer1,
      ttl = 60 * 60 * 24,
      description = "some_description")

    val streamForProducer2: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorageInstForProducer2,
      dataStorage = cassandraInstForProducer2,
      lockService = lockerFactoryForProducer2,
      ttl = 60 * 60 * 24,
      description = "some_description")

    val streamForConsumer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorageInstForConsumer,
      dataStorage = cassandraInstForConsumer,
      lockService = lockerFactoryForConsumer,
      ttl = 60 * 60 * 24,
      description = "some_description")

    //options
    val producerOptions1 = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(streamForProducer1, List(0,1,2)),
      stringToArrayByteConverter)

    val producerOptions2 = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(streamForProducer2, List(0,1,2)),
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
    producer1 = new BasicProducer("test_producer", streamForProducer1, producerOptions1)
    producer2 = new BasicProducer("test_producer", streamForProducer2, producerOptions2)
    consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
  }

  "two producers, consumer" should "first producer - generate transactions lazily, second producer - generate transactions faster" +
    " than the first one but with pause at the very beginning, consumer - retrieve all transactions which was sent" in {
    val timeoutForWaiting = 120
    val totalElementsInTxn = 10
    val dataToSend1: List[String] = (for (part <- 0 until totalElementsInTxn) yield "data_to_send_pr1_" + randomString).toList.sorted
    val dataToSend2: List[String] = (for (part <- 0 until totalElementsInTxn) yield "data_to_send_pr2_" + randomString).toList.sorted

    val producer1Thread = new Thread(new Runnable {
      def run() {
        val txn = producer1.newTransaction(false)
        dataToSend1.foreach { x =>
          txn.send(x)
          Thread.sleep(2000)
        }
        txn.close()
      }
    })

    val producer2Thread = new Thread(new Runnable {
      def run() {
        Thread.sleep(2000)
        val txn = producer2.newTransaction(false)
        dataToSend2.foreach{ x=>
          txn.send(x)
        }
        txn.close()
      }
    })

    var checkVal = true

    val consumerThread = new Thread(new Runnable {
      Thread.sleep(3000)
      def run() = {
        var isFirstProducerFinished = true
        breakable{ while(true) {
          val txnOpt = consumer.getTransaction
          if (txnOpt.isDefined) {
            val data = txnOpt.get.getAll().sorted
            if (isFirstProducerFinished) {
              checkVal &= data == dataToSend1
              isFirstProducerFinished = false
            }
            else {
              checkVal &= data == dataToSend2
              break()
            }
          }
          Thread.sleep(200)
        }}
      }
    })

    producer1Thread.start()
    producer2Thread.start()
    consumerThread.start()
    producer1Thread.join(timeoutForWaiting*1000)
    producer2Thread.join(timeoutForWaiting*1000)
    consumerThread.join(timeoutForWaiting*1000)

    checkVal &= !producer1Thread.isAlive
    checkVal &= !producer2Thread.isAlive
    checkVal &= !consumerThread.isAlive

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _=>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    lockerFactoryForConsumer.closeFactory()
    lockerFactoryForProducer1.closeFactory()
    lockerFactoryForProducer2.closeFactory()
  }
}