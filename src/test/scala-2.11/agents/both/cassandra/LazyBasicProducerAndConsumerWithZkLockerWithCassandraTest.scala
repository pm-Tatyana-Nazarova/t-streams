package agents.both.cassandra

import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.lockservice.impl.{ZkLockerFactory, ZkServer}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Cluster, Session}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{CassandraEntities, RandomStringGen}
import scala.util.control.Breaks._


class LazyBasicProducerAndConsumerWithZkLockerWithCassandraTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntities.createDataTable(temporarySession, randomKeyspace)
  }

  "two producers and consumer" should "first producer create transaction and start filling it lazily," +
    " second producer create transaction and start filling it faster than the first one," +
    " consumer should start in time when first producer has not finished working but second already has done it job" in {

    val totalElements = 10
    val dataToSend1: List[String] = (for (part <- 0 until totalElements) yield "data_to_send_pr1_" + part).toList.sorted
    val dataToSend2: List[String] = (for (part <- 0 until totalElements) yield "data_to_send_pr2_" + part).toList.sorted
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new CassandraStorageFactory
    val arrayByteToStringConverter = new ArrayByteToStringConverter
    val stringToArrayByteConverter = new StringToArrayByteConverter
    val lockService1 = new ZkLockerFactory(List(ZkServer("localhost", 2181)), "/some_path", 10)
    val lockService2 = new ZkLockerFactory(List(ZkServer("localhost", 2181)), "/some_path", 10)
    val lockService3 = new ZkLockerFactory(List(ZkServer("localhost", 2181)), "/some_path", 10)
    val cassandraOptions = new CassandraStorageOptions(List("localhost"), randomKeyspace)

    val streamConsumer: BasicStream[Array[Byte]] = BasicStreamService.createStream(
      streamName = "test_stream",
      partitions = 3,
      ttl = 60 * 60 * 24,
      description = "unit_testing",
      metadataStorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace),
      dataStorage = storageFactory.getInstance(cassandraOptions), lockService1)

    val streamProducer1: BasicStream[Array[Byte]] = BasicStreamService.loadStream(
      streamName = "test_stream",
      metadataStorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace),
      dataStorage = storageFactory.getInstance(cassandraOptions), lockService2).get

    val streamProducer2: BasicStream[Array[Byte]] = BasicStreamService.loadStream(
      streamName = "test_stream",
      metadataStorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace),
      dataStorage = storageFactory.getInstance(cassandraOptions), lockService3).get

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      PolicyRepository.getRoundRobinPolicy(
        usedPartitions = List(0,1,2),
        stream = streamConsumer),
      Oldest,
      useLastOffset = false)

    val producerOptions1 = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(
        usedPartitions = List(0,1,2),
        stream = streamConsumer),
      stringToArrayByteConverter)

    val producerOptions2 = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(
        usedPartitions = List(0,1,2),
        stream = streamConsumer),
      stringToArrayByteConverter)

    val producer1 = new BasicProducer("test_producer1", streamProducer1, producerOptions1)
    val producer2 = new BasicProducer("test_producer2", streamProducer2, producerOptions2)
    val consumer = new BasicConsumer("test_consumer", streamConsumer, consumerOptions)

    val producer1Thread = new Thread( new Runnable {
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
    producer1Thread.join(40*1000)
    producer2Thread.join(40*1000)
    consumerThread.join(40*1000)

    checkVal && !producer1Thread.isAlive && !producer2Thread.isAlive && !consumerThread.isAlive shouldEqual true
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
