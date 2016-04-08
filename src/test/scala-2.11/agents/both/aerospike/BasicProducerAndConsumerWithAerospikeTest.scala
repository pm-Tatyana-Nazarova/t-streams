package agents.both.aerospike

import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{BasicConsumerTransaction, BasicConsumerOptions, BasicConsumer}
import com.bwsw.tstreams.agents.producer.{BasicProducerOptions, BasicProducer}
import com.bwsw.tstreams.converter.{StringToArrayByteConverter, ArrayByteToStringConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.lockservice.impl.ZkLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class BasicProducerAndConsumerWithAerospikeTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var consumer : BasicConsumer[Array[Byte],String] = null
  var producer : BasicProducer[String,Array[Byte]] = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntities.createDataTable(temporarySession, randomKeyspace)
    
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new AerospikeStorageFactory
    val arrayByteToStringConverter = new ArrayByteToStringConverter
    val stringToArrayByteConverter = new StringToArrayByteConverter
    val hosts = List(new Host("localhost",3000),new Host("localhost",3001),new Host("localhost",3002),new Host("localhost",3003))
    val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
    val lockService = new ZkLockerFactory(List(new InetSocketAddress("localhost", 2181)), "/some_path", 10)
    val streamForProducer: BasicStream[Array[Byte]] = BasicStreamService.createStream(
      streamName = "test_stream",
      partitions = 3,
      ttl = 60 * 60 * 24,
      description = "unit_testing",
      metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
      dataStorage = storageFactory.getInstance(aerospikeOptions),
      lockService = lockService)

    val streamForConsumer: BasicStream[Array[Byte]] = BasicStreamService.loadStream(
      streamName = "test_stream",
      metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
      dataStorage = storageFactory.getInstance(aerospikeOptions),
      lockService = null).get

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      PolicyRepository.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
      Oldest,
      useLastOffset = false)

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(streamForProducer, List(0,1,2)),
      stringToArrayByteConverter)

    consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
    producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  }

  "producer, consumer" should "producer:generate one transaction, consumer:retrieve it(with getAll method)" in {
    CassandraEntities.clearTables(temporarySession, randomKeyspace)
    val totalDataInTxn = 10
    val producerTransaction = producer.newTransaction(false)
    val sendData: List[String] = (for (part <- 0 until totalDataInTxn) yield "data_part_" + part).toList.sorted
    sendData.foreach{ x=>
      producerTransaction.send(x)
    }
    producerTransaction.close()
    val txnOpt = consumer.getTransaction
    assert(txnOpt.isDefined)
    val txn = txnOpt.get
    txn.getAll().sorted shouldEqual sendData
  }

  "producer, consumer" should "producer:generate one transaction, consumer:retrieve it(using iterator)" in {
    CassandraEntities.clearTables(temporarySession, randomKeyspace)
    val totalDataInTxn = 10
    val producerTransaction = producer.newTransaction(false)
    val sendData: List[String] = (for (part <- 0 until totalDataInTxn) yield "data_part_" + part).toList.sorted
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
    dataToAssert.toList.sorted shouldEqual sendData
  }

  "producer, consumer" should "producer:generate some set of transactions, consumer: retrieve them all" in {
    CassandraEntities.clearTables(temporarySession, randomKeyspace)
    val totalTxn = 10
    val totalDataInTxn = 10000
    val sendData: List[String] = (for (part <- 0 until totalDataInTxn) yield "data_part_" + part).toList.sorted
    (0 until totalTxn).foreach {
      _=>
        val producerTransaction = producer.newTransaction(false)
        sendData.foreach{ x=>
          producerTransaction.send(x)
        }
        producerTransaction.close()
    }
    (0 until totalTxn).foreach {
      _=>
        val txn = consumer.getTransaction
        assert(txn.nonEmpty)
        assert(txn.get.getAll().sorted == sendData)
    }
  }

  "producer, consumer" should "producer:generate transaction, consumer retrieve it (in parallel)" in {
    val totalDataInTxn = 10
    CassandraEntities.clearTables(temporarySession, randomKeyspace)
    val sendData: List[String] = (for (part <- 0 until totalDataInTxn) yield "data_part_" + part).toList.sorted

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

    val consumerThread = new Thread(new Runnable {
      def run() {
        breakable{while(true) {
          val consumedTxn: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getTransaction
          if (consumedTxn.isDefined) {
            assert(consumedTxn.get.getAll().sorted == sendData)
            break()
          }
          Thread.sleep(1000)
        }}
      }
    })

    producerThread.start()
    consumerThread.start()
    producerThread.join(40*1000)
    consumerThread.join(40*1000)

    producerThread.isAlive || consumerThread.isAlive shouldEqual false
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}

