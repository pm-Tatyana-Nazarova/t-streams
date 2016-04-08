package agents.both.aerospike

import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Cluster, Session}
import org.redisson.Config
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{CassandraEntities, RandomStringGen}


class SomeAmountOfBasicProducersStreamingInOnePartitionWithAerospikeTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var aerospikeOptions : AerospikeStorageOptions = null
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter


  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntities.createDataTable(temporarySession, randomKeyspace)
    val hosts = List(new Host("localhost",3000),new Host("localhost",3001),new Host("localhost",3002),new Host("localhost",3003))
    aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  }

  "Some amount of producers and one consumer" should "send transactions and retrieve them all" in {
    val totalTxn = 10
    val totalElementsInTxn = 10
    val producersAmount = 15
    val dataToSend: List[String] = (for (part <- 0 until totalElementsInTxn) yield randomString).toList.sorted
    val producers: List[BasicProducer[String, Array[Byte]]] = (0 until producersAmount).toList.map(x=>getProducer)
    val producersThreads = producers.map(p =>
      new Thread(new Runnable {
        def run(){
          var i = 0
          while(i < totalTxn) {
            Thread.sleep(2000)
            val txn = p.newTransaction(false)
            dataToSend.foreach(x => txn.send(x))
            txn.close()
            i+=1
          }
        }
      }))

    val streamInst = getStream

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      PolicyRepository.getRoundRobinPolicy(
        usedPartitions = List(0),
        stream = streamInst),
      Oldest,
      useLastOffset = false)

    var checkVal = true

    val consumerThread = new Thread(
      new Runnable {
      Thread.sleep(3000)
        val consumer = new BasicConsumer("test_consumer", streamInst, consumerOptions)
        def run() = {
        var i = 0
        while(i < totalTxn*producersAmount) {
          val txn = consumer.getTransaction
          if (txn.isDefined){
            checkVal &= txn.get.getAll().sorted == dataToSend
            assert(txn.get.getAll().sorted == dataToSend)
            i+=1
          }
          Thread.sleep(200)
        }
      }
    })

    producersThreads.foreach(x=>x.start())
    consumerThread.start()
    consumerThread.join(120 * 1000)
    producersThreads.foreach(x=>x.join(120 * 1000))

    checkVal &= !consumerThread.isAlive
    producersThreads.foreach(x=> checkVal &= !x.isAlive)

    checkVal shouldEqual true
  }

  def getProducer : BasicProducer[String,Array[Byte]] = {
    val stream = getStream
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(
        usedPartitions = List(0),
        stream = stream),
      stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream: BasicStream[Array[Byte]] = {
    val config = new Config()
    config.useSingleServer().setAddress("localhost:6379")
    val lockService1 = new RedisLockerFactory("/unittest", config)
    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = 1,
      metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
      dataStorage = storageFactory.getInstance(aerospikeOptions),
      lockService = lockService1,
      ttl = 60 * 60 * 24,
      description = "some_description")
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}