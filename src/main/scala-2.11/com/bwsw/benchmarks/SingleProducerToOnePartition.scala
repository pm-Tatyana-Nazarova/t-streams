package com.bwsw.benchmarks

import java.io.File
import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.PolicyRepository
import com.bwsw.tstreams.streams.BasicStream
import org.redisson.Config
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random
import org.json4s._
import org.json4s.jackson.JsonMethods._


object SingleProducerToOnePartition extends MetricsCalculator with MetadataCreator{
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats

    val inputString: String = Source.fromFile("benchmark_config").getLines().toList.mkString("")
    val parsed = parse(inputString)
    //constants
    val init = (parsed \\ "InitMetadata").extract[Boolean]
    val size = (parsed \\ "Size").extract[Int]
    val amountInTxn = (parsed \\ "AmountInTxn").extract[Int]
    val totalTxn = (parsed \\ "TotalTxn").extract[Int]
    val interval = (parsed \\ "TxnInterval").extract[Int]
    //spaces
    val cassandraKeyspace = (parsed \\ "Cassandra" \\ "Keyspace").extract[String]
    val aerospikeNamespace = (parsed \\ "Aerospike" \\ "Namespace").extract[String]
    //hosts configs
    val aerospikeHosts = (parsed \\ "Aerospike" \\ "Hosts").extract[List[Map[String,Int]]]
    val cassandraHosts = (parsed \\ "Cassandra" \\ "Hosts").extract[List[String]]
    val redisHosts = (parsed \\ "Redis" \\ "Hosts").extract[List[Map[String,Int]]]

    //run it only ones
    if (init)
      initMetadata(cassandraHosts, cassandraKeyspace)

    //start benchmark
    val dataToSend = Random.alphanumeric.take(size).mkString("")
    val metadataFactory = new MetadataStorageFactory
    val storageFactory = new AerospikeStorageFactory

    val convertedHosts = aerospikeHosts.map{ x =>
      val hostAndPort: (String, Int) = x.toList.head
      new Host(hostAndPort._1, hostAndPort._2)
    }

    val aerospikeOptions = new AerospikeStorageOptions(
      "test",
      convertedHosts)

    val metadataInst = metadataFactory.getInstance(cassandraHosts, cassandraKeyspace)
    val storageInst = storageFactory.getInstance(aerospikeOptions)

    val convertedRedisHosts = redisHosts.map{ x=>
      val hostAndPort = x.toList.head
      s"${hostAndPort._1}:${hostAndPort._2}"
    }
    val config = new Config()
    if (redisHosts.size == 1){
      config.useSingleServer().setAddress(convertedRedisHosts.head)
    }
    else{
      //TODO not tested
      val masterName = (parsed \\ "Redis" \\ "MasterName").extract[String]
      config
        .useSentinelServers()
        .setMasterName(masterName)
        .addSentinelAddress(convertedRedisHosts:_*)
    }

    val lockerFactory = new RedisLockerFactory("/benchmark_path", config)

    val stream = new BasicStream[Array[Byte]](
      name = "benchmark_stream",
      partitions = 1,
      metadataStorage = metadataInst,
      dataStorage = storageInst,
      lockService = lockerFactory,
      ttl = 60 * 60 * 24,
      description = "some_description")

    val stringToArrayByteConverter = new StringToArrayByteConverter
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 60,
      transactionKeepAliveInterval = 20,
      producerKeepAliveInterval = 1,
      PolicyRepository.getRoundRobinPolicy(
        stream, List(0)), //here we stream only in the one partition
      stringToArrayByteConverter)

    val producer: BasicProducer[String, Array[Byte]] = new BasicProducer("some_producer_name", stream, producerOptions)

    var tstart = System.nanoTime()

    var timeInAllTxn = ListBuffer[Long]()

    (0 until totalTxn).foreach{ cur =>
      val txn = producer.newTransaction(false)
      (0 until amountInTxn).foreach{ _ =>
        txn.send(dataToSend)
      }
      txn.close()

      if (cur % interval == 0){
        val newtstart = System.nanoTime()
        val difference = (newtstart - tstart)/(1000 * 1000 * 1000)
        tstart = newtstart
        timeInAllTxn += difference

        println(s"timePerTxnBatch=$difference sec")
      }
    }

    val avg = getMedian(timeInAllTxn.toList)
    val perc = getPercentile(timeInAllTxn.toList, 0.95)
    val variance = getVariance(timeInAllTxn.toList)
    val sum = getSum(timeInAllTxn.toList)

    val file1 = new File("total_statistic")
    val file2 = new File("pertransaction_statistic")
    val pw1 = new java.io.PrintWriter(file1)
    val pw2 = new java.io.PrintWriter(file2)

    pw1.write(s"totalTxn=$totalTxn\n")
    pw1.write(s"dataInTxn=$amountInTxn\n")
    pw1.write(s"sizeOfDataInTxn=$size\n\n")
    pw1.write(s"avg=$avg\n")
    pw1.write(s"percentile=$perc\n")
    pw1.write(s"variance=$variance\n")
    pw1.write(s"sum=$sum\n")
    pw1.close()

    timeInAllTxn.foreach {x=>
      pw2.write(s"time=${x.toString}sec\n")
    }
    pw2.close()

    metadataFactory.closeFactory()
    storageFactory.closeFactory()
    lockerFactory.closeFactory()
  }
}
