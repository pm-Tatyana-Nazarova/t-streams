package agents.both.batch_insert.cassandra

import java.net.InetSocketAddress

import agents.both.batch_insert.BatchSizeTestVal
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
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


class CManyBasicProducersStreamingInOnePartitionAndConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with BatchSizeTestVal{
  //creating keyspace, metadata
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

  //coordinator for coordinating producer/consumer
  val config = new Config()
  config.useSingleServer().setAddress("localhost:6379")
  val redissonClient = Redisson.create(config)
  val coordinator = new Coordinator("some_path", redissonClient)

  //cassandra storage options
  val cassandraStorageOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)

  "Some amount of producers and one consumer" should "producers - send transactions in one partition and consumer - retrieve them all" in {
    val timeoutForWaiting = 60*5
    val totalTxn = 10
    val totalElementsInTxn = 10
    val producersAmount = 15
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted
    val producers: List[BasicProducer[String, Array[Byte]]] = (0 until producersAmount).toList.map(x=>getProducer)
    val producersThreads = producers.map(p =>
      new Thread(new Runnable {
        def run(){
          var i = 0
          while(i < totalTxn) {
            Thread.sleep(2000)
            val txn = p.newTransaction(ProducerPolicies.errorIfOpen)
            dataToSend.foreach(x => txn.send(x))
            txn.checkpoint()
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
      RoundRobinPolicyCreator.getRoundRobinPolicy(
        usedPartitions = List(0),
        stream = streamInst),
      Oldest,
      LocalGeneratorCreator.getGen(),
      useLastOffset = false)

    var checkVal = true

    val consumer = new BasicConsumer("test_consumer", streamInst, consumerOptions)

    val consumerThread = new Thread(
      new Runnable {
      Thread.sleep(3000)
        def run() = {
        var i = 0
        while(i < totalTxn*producersAmount) {
          val txn = consumer.getTransaction
          if (txn.isDefined){
            checkVal &= txn.get.getAll().sorted == dataToSend
            i+=1
          }
          Thread.sleep(200)
        }
      }
    })

    producersThreads.foreach(x=>x.start())
    consumerThread.start()
    consumerThread.join(timeoutForWaiting * 1000)
    producersThreads.foreach(x=>x.join(timeoutForWaiting * 1000))

    //assert that is nothing to read
    checkVal &= consumer.getTransaction.isEmpty

    checkVal &= !consumerThread.isAlive
    producersThreads.foreach(x=> checkVal &= !x.isAlive)

    checkVal shouldEqual true
  }

  def getProducer: BasicProducer[String,Array[Byte]] = {
    val stream = getStream
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0)),
      BatchInsert(batchSizeVal),
      LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream: BasicStream[Array[Byte]] = {
    //storage instances
    val metadataStorageInst = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val dataStorageInst = storageFactory.getInstance(cassandraStorageOptions)

    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = 1,
      metadataStorage = metadataStorageInst,
      dataStorage = dataStorageInst,
      coordinator = coordinator,
      ttl = 60 * 10,
      description = "some_description")
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