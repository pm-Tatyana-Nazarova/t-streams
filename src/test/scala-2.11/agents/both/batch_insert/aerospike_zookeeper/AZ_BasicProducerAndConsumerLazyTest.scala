package agents.both.batch_insert.aerospike_zookeeper

import java.net.InetSocketAddress

import agents.both.batch_insert.BatchSizeTestVal
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{ProducerPolicies, BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.lockservice.impl.ZkLockServiceFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{RoundRobinPolicyCreator, LocalGeneratorCreator, CassandraHelper, RandomStringGen}

import scala.util.control.Breaks._


class AZ_BasicProducerAndConsumerLazyTest extends FlatSpec with Matchers with BeforeAndAfterAll with BatchSizeTestVal{
  def randomString: String = RandomStringGen.randomAlphaString(10)

  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)

  //factories for storages creation
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory

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
  val aerospikeInstForProducer1 = storageFactory.getInstance(aerospikeOptions)
  val aerospikeInstForProducer2 = storageFactory.getInstance(aerospikeOptions)
  val aerospikeInstForConsumer = storageFactory.getInstance(aerospikeOptions)

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
  val lockerFactoryForProducer1 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)), "/some_path", 10)
  val lockerFactoryForProducer2 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)), "/some_path", 10)
  val lockerFactoryForConsumer = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)), "/some_path", 10)

  //streams
  val streamForProducer1: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForProducer1,
    dataStorage = aerospikeInstForProducer1,
    lockService = lockerFactoryForProducer1,
    ttl = 60 * 10,
    description = "some_description")

  val streamForProducer2: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForProducer2,
    dataStorage = aerospikeInstForProducer2,
    lockService = lockerFactoryForProducer2,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = aerospikeInstForConsumer,
    lockService = lockerFactoryForConsumer,
    ttl = 60 * 10,
    description = "some_description")

  //options
  val producerOptions1 = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 6,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer1, List(0,1,2)),
    BatchInsert(batchSizeVal),
    LocalGeneratorCreator.getGen(),
    stringToArrayByteConverter)

  val producerOptions2 = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 6,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer2, List(0,1,2)),
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
    useLastOffset = false)

  //agents
  val producer1 = new BasicProducer("test_producer", streamForProducer1, producerOptions1)
  val producer2 = new BasicProducer("test_producer", streamForProducer2, producerOptions2)
  val consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)


  "two producers, consumer" should "first producer - generate transactions lazily, second producer - generate transactions faster" +
    " than the first one but with pause at the very beginning, consumer - retrieve all transactions which was sent" in {
    val timeoutForWaiting = 120
    val totalElementsInTxn = 10
    val dataToSend1: List[String] = (for (part <- 0 until totalElementsInTxn) yield "data_to_send_pr1_" + randomString).toList.sorted
    val dataToSend2: List[String] = (for (part <- 0 until totalElementsInTxn) yield "data_to_send_pr2_" + randomString).toList.sorted

    val producer1Thread = new Thread(new Runnable {
      def run() {
        val txn = producer1.newTransaction(ProducerPolicies.errorIfOpen)
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
        val txn = producer2.newTransaction(ProducerPolicies.errorIfOpen)
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