package agents.both.aerospike_redis

import java.net.InetSocketAddress
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.{BasicProducerOptions, BasicProducer}
import com.bwsw.tstreams.converter.{StringToArrayByteConverter, ArrayByteToStringConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
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


class AR_ManyBasicProducersStreamingInManyPartitionsAndConsumerWithCheckpointsTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var cluster : Cluster = null
  var session: Session = null
  //storage options
  var aerospikeOptions : AerospikeStorageOptions = null
  //factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory
  //converters
  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter
  //all locker factory instances
  var instances = ListBuffer[RedisLockerFactory]()


  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    cluster = Cluster.builder().addContactPoint("localhost").build()
    session = cluster.connect()
    CassandraHelper.createKeyspace(session, randomKeyspace)
    CassandraHelper.createMetadataTables(session, randomKeyspace)

    val hosts = List(
      new Host("localhost",3000),
      new Host("localhost",3001),
      new Host("localhost",3002),
      new Host("localhost",3003))
    aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  }

  "Some amount of producers and one consumer" should "producers - send transactions in many partition" +
    " (each producer send each txn in only one partition without intersection " +
    " for ex. producer1 in partition1, producer2 in partition2, producer3 in partition3 etc...) " +
    " consumer - retrieve them all with reinitialization every 10 transactions" in {
    val timeoutForWaiting = 60*5
    val totalPartitions = 100
    val totalTxn = 10
    val totalElementsInTxn = 10
    val producersAmount = 15
    val dataToSend = (for (part <- 0 until totalElementsInTxn) yield randomString).sorted

    val producers: List[BasicProducer[String, Array[Byte]]] =
      (0 until producersAmount)
        .toList
        .map(x=>getProducer(List(x),totalPartitions))

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

    val streamInst = getStream(100)

    val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
      transactionsPreload = 10,
      dataPreload = 7,
      consumerKeepAliveInterval = 5,
      arrayByteToStringConverter,
      PolicyRepository.getRoundRobinPolicy(
        usedPartitions = (0 until 100).toList,
        stream = streamInst),
      Oldest,
      useLastOffset = true)

    var checkVal = true

    var consumer = new BasicConsumer("test_consumer", streamInst, consumerOptions)

    val consumerThread = new Thread(
      new Runnable {
        Thread.sleep(3000)
        def run() = {
          var i = 0
          while(i < totalTxn*producersAmount) {

            //every 10 txns consumer start reinitializing
            if (i % 10 == 0) {
              consumer = new BasicConsumer("test_consumer", streamInst, consumerOptions)
              Thread.sleep(1000)
            }

            val txn = consumer.getTransaction

            if (txn.isDefined){
              checkVal &= txn.get.getAll().sorted == dataToSend
              consumer.checkpoint()
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

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      writePolicy = PolicyRepository.getRoundRobinPolicy(stream, usedPartitions),
      converter = stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer1", stream, producerOptions)
    producer
  }

  def getStream(partitions : Int): BasicStream[Array[Byte]] = {
    //locker factory instance
    val config = new Config()
    config.useSingleServer().setAddress("localhost:6379")
    val lockService = new RedisLockerFactory("/some_path", config)
    instances += lockService

    //storage instances
    val metadataStorageInst = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)
    val dataStorageInst = storageFactory.getInstance(aerospikeOptions)

    new BasicStream[Array[Byte]](
      name = "stream_name",
      partitions = partitions,
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
