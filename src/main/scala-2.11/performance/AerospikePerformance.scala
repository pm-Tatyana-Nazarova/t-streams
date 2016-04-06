package performance

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.{Cluster, Session}
import org.redisson.Config

import scala.collection.mutable.ListBuffer


object AerospikePerformance {
  def main(args: Array[String]) {
    val randomKeyspace : String = "performance_test"
    var temporaryCluster : Cluster = null
    var temporarySession: Session = null
    var producer : BasicProducer[String,Array[Byte]] = null

    //creating tables
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()
    try {
      temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    }
    catch{
      case e : Exception => //do nothing here in case if keyspace not exist
    }

    CassandraEntitiesCreator.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntitiesCreator.createMetadataTables(temporarySession, randomKeyspace)
    CassandraEntitiesCreator.createDataTable(temporarySession, randomKeyspace)

    //start creating producer and consumer
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new AerospikeStorageFactory
    val stringToArrayByteConverter = new StringToArrayByteConverter

    val hosts = List(new Host("localhost",3000),new Host("localhost",3001),new Host("localhost",3002),new Host("localhost",3003))
    val aerospikeOptions = new AerospikeStorageOptions("test", hosts)

    val conf: Config = new Config()
    conf.useSingleServer().setAddress("localhost:6379")
    val redisLockerFactory = new RedisLockerFactory("/unitpath", conf)

    val stream: BasicStream[Array[Byte]] = BasicStreamService.createStream(
      streamName = "test_stream",
      partitions = 5,
      ttl = 60 * 60 * 24,
      description = "unit_testing",
      metadataStorage = metadataStorageFactory.getInstance(List("localhost"), randomKeyspace),
      dataStorage = storageFactory.getInstance(aerospikeOptions),
      lockService = redisLockerFactory)

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(stream, List(0,2,4)),
      stringToArrayByteConverter)

    producer = new BasicProducer("test_producer", stream, producerOptions)

    val string = "ajskdlajdkajskdlajdkajskdlajdkajskdlajdkajskdlajdkajskdlajdkajskdlajdkajskdlajdkajskdlajdkajskdlajdk"

    val (txnCnt, dataToSend) = {
      assert(args.length == 2, "incorrect app args")
      val dataToSend = new ListBuffer[String]()
      for (i <- 0 until args(1).toInt)
        dataToSend += string

      (args(0).toInt, dataToSend)
    }


    var t0 = System.nanoTime()
    println(s"startTime=${java.util.Calendar.getInstance().getTime}")
    for (i <- 0 until txnCnt){
      val txn = producer.newTransaction(false)
      dataToSend.foreach(x=> txn.send(x))
      txn.close()
      if (i%10000 == 0) {
        val newTime = System.nanoTime()
        val diff = newTime - t0
        t0 = newTime
        println("time=" + (diff / (1000 * 1000 * 1000)) + s"sec, txnnum=$i")
      }
    }
    println(s"endTime=${java.util.Calendar.getInstance().getTime}")

    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
    redisLockerFactory.closeFactory()
  }
}
