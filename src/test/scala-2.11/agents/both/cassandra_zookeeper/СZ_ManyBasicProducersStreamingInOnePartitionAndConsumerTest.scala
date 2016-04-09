package agents.both.cassandra_zookeeper

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.entities.offsets.Oldest
import com.bwsw.tstreams.lockservice.impl.ZkLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Cluster, Session}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{CassandraHelper, RandomStringGen}
import scala.collection.mutable.ListBuffer


class СZ_ManyBasicProducersStreamingInOnePartitionAndConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var cluster : Cluster = null
  var session: Session = null
  //storage options
  var cassandraOptions : CassandraStorageOptions = null
  //factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory
  //converters
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter
  //all locker factory instances
  var instances = ListBuffer[ZkLockerFactory]()


  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    cluster = Cluster.builder().addContactPoint("localhost").build()
    session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)
    CassandraHelper.createDataTable(session, randomKeyspace)

    cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)
  }

  "Some amount of producers and one consumer" should "producers - send transactions in one partition and consumer - retrieve them all" in {
    val timeoutForWaiting = 60*5
    val totalTxn = 10
    val totalElementsInTxn = 10
    val producersAmount = 15
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted
    val producers: List[BasicProducer[String, Array[Byte]]] = (0 until producersAmount).toList.map(x=>getProducer())
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

    val streamInst = getStream()

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

  def getProducer() : BasicProducer[String,Array[Byte]] = {
    val stream = getStream()
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      writePolicy = PolicyRepository.getRoundRobinPolicy(stream, List(0)),
      converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream(): BasicStream[Array[Byte]] = {
    //locker factory instance
    val lockService = new ZkLockerFactory(List(new InetSocketAddress("localhost",2181)), "/some_path", 10)
    instances += lockService

    //storage instances
    val metadataStorageInst = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val dataStorageInst = storageFactory.getInstance(cassandraOptions)

    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = 1,
      metadataStorage = metadataStorageInst,
      dataStorage = dataStorageInst,
      lockService = lockService,
      ttl = 60 * 60 * 24,
      description = "some_description")
  }

  override def afterAll(): Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    instances.foreach(x=>x.closeFactory())
  }
}