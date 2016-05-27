package agents.both.batch_insert.cassandra

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerCoordinationSettings, BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{ProducerCoordinationSettings, BasicProducer, BasicProducerOptions, ProducerPolicies}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class CManyBasicProducersStreamingInManyPartitionsAndConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  var port = 8000

  //creating keyspace, metadata, data
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

  //cassandra storage options
  val cassandraStorageOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)

  "Some amount of producers and one consumer" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one partition without intersection " +
    " for ex. producer1 in partition1, producer2 in partition2, producer3 in partition3 etc...)," +
    " consumer - retrieve them all" in {
    val timeoutForWaiting = 60*5
    val totalPartitions = 4
    val totalTxn = 10
    val totalElementsInTxn = 3
    val producersAmount = 10
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted

    val producers: List[BasicProducer[String, Array[Byte]]] =
      (0 until producersAmount)
        .toList
        .map(x=>getProducer(List(x%totalPartitions),totalPartitions))

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

    val streamInst = getStream(totalPartitions)

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      RoundRobinPolicyCreator.getRoundRobinPolicy(
        usedPartitions = (0 until totalPartitions).toList,
        stream = streamInst),
      new ConsumerCoordinationSettings("localhost:8588", "/unit", List(new InetSocketAddress("localhost",2181)), 7000),
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
    (0 until totalPartitions) foreach { _=>
      checkVal &= consumer.getTransaction.isEmpty
    }

    checkVal &= !consumerThread.isAlive
    producersThreads.foreach(x=> checkVal &= !x.isAlive)

    checkVal shouldEqual true
  }

  def getProducer(usedPartitions : List[Int], totalPartitions : Int) : BasicProducer[String,Array[Byte]] = {
    val stream = getStream(totalPartitions)

    val agentSettings = new ProducerCoordinationSettings(
      agentAddress = s"localhost:$port",
      zkHosts = List(new InetSocketAddress("localhost", 2181)),
      zkRootPath = "/unit",
      zkTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5)

    port += 1

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, usedPartitions),
      BatchInsert(batchSizeVal),
      LocalGeneratorCreator.getGen(),
      agentSettings,
      converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream(partitions : Int): BasicStream[Array[Byte]] = {
    //storage instances
    val metadataStorageInst = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val dataStorageInst = storageFactory.getInstance(cassandraStorageOptions)
    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = partitions,
      metadataStorage = metadataStorageInst,
      dataStorage = dataStorageInst,
      ttl = 60 * 10,
      description = "some_description")
  }

  override def afterAll(): Unit = {
    val zkService = new ZkService("/unit", List(new InetSocketAddress("localhost",2181)), 7000)
    zkService.deleteRecursive("")
    zkService.close()
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}