package agents.producer

import com.bwsw.tstreams.agents.producer.{BasicProducerTransaction, BasicProducerOptions, BasicProducer}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory, CassandraStorage}
import com.bwsw.tstreams.lockservice.impl.{ZkServer, ZkLockerFactory}
import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.datastax.driver.core.{Session, Cluster}
import com.gilt.timeuuid.TimeUuid
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}
import scala.collection.mutable


class BasicProducerTransactionTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)
  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var producer : BasicProducer[String,Array[Byte]] = null
  var storage : CassandraStorage = null
  var mstorage : MetadataStorage = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntities.createDataTable(temporarySession, randomKeyspace)

    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new CassandraStorageFactory
    val stringToArrayByteConverter = new StringToArrayByteConverter
    val lockService = new ZkLockerFactory(List(ZkServer("localhost", 2181)), "/some_path", 10)
    val cassandraOptions = new CassandraStorageOptions(List("localhost"), randomKeyspace)

    mstorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace)
    storage = storageFactory.getInstance(cassandraOptions)

    val stream = BasicStreamService.createStream("test_stream", 3, 60*60*24, "unit_testing", mstorage, storage, lockService)

    val options = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 3,
      transactionKeepAliveInterval = 1,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(stream,List(0,1,2)),
      stringToArrayByteConverter)

    producer = new BasicProducer("test_producer", stream, options)
  }

  "BasicProducerTransaction.send BasicProducerTransaction.close" should "send transaction in storage and update commit log despite of pauses" +
    "between transaction parts" in {
    CassandraEntities.clearTables(temporarySession, randomKeyspace)

    val txn: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    val sendData: List[String] = (for (part <-0 until 2) yield "data_part_" + part).toList
    sendData.foreach{ x=>
      txn.send(x)
      Thread.sleep(6000)
    }
    txn.close()
    Thread.sleep(6000)

    val q1 = mstorage.commitEntity.getTransactions(producer.stream.name, 0, TimeUuid(0), 1)
    val q2 = mstorage.commitEntity.getTransactions(producer.stream.name, 1, TimeUuid(0), 1)
    val q3 = mstorage.commitEntity.getTransactions(producer.stream.name, 2, TimeUuid(0), 1)

    val totalQueue = q1 ++ q2 ++ q3
    assert(totalQueue.size == 1)

    val data1: mutable.Queue[Array[Byte]] = storage.get(producer.stream.getName, 0,  totalQueue.head.time, 0, 1)
    val data2: mutable.Queue[Array[Byte]] = storage.get(producer.stream.getName, 1,  totalQueue.head.time, 0, 1)
    val data3: mutable.Queue[Array[Byte]] = storage.get(producer.stream.getName, 2,  totalQueue.head.time, 0, 1)

    val totalData = data1 ++ data2 ++ data3

    assert(totalData.size == sendData.size)
    assert(totalData.toList.map(x=>new String(x)).sorted == sendData.sorted)
  }

  "BasicProducerTransaction.send BasicProducerTransaction.close" should "send transaction in storage and update commit log" in {
    val totTxnAmount = 5000

    CassandraEntities.clearTables(temporarySession, randomKeyspace)

    val txn: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(false)
    val sendData: List[String] = (for (part <-0 until totTxnAmount) yield "data_part_" + part).toList
    sendData.foreach{ x=>
      txn.send(x)
    }
    txn.close()

    val q1 = mstorage.commitEntity.getTransactions(producer.stream.name, 0, TimeUuid(0), 1)
    val q2 = mstorage.commitEntity.getTransactions(producer.stream.name, 1, TimeUuid(0), 1)
    val q3 = mstorage.commitEntity.getTransactions(producer.stream.name, 2, TimeUuid(0), 1)

    val totalQueue = q1 ++ q2 ++ q3
    assert(totalQueue.size == 1)

    val data1: mutable.Queue[Array[Byte]] = storage.get(producer.stream.getName, 0,  totalQueue.head.time, 0, totTxnAmount-1)
    val data2: mutable.Queue[Array[Byte]] = storage.get(producer.stream.getName, 1,  totalQueue.head.time, 0, totTxnAmount-1)
    val data3: mutable.Queue[Array[Byte]] = storage.get(producer.stream.getName, 2,  totalQueue.head.time, 0, totTxnAmount-1)

    val totalData = data1 ++ data2 ++ data3

    assert(totalData.size == sendData.size)
    assert(totalData.toList.map(x=>new String(x)).sorted == sendData.sorted)
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
