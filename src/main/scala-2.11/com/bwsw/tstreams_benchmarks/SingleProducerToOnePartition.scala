package com.bwsw.tstreams_benchmarks

import java.io.File
import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams_benchmarks.utils.JsonUtils
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, BatchInsert, SingleElementInsert}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.RoundRobinPolicy
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.txngenerator.LocalTimeTxnGenerator
import org.redisson.Config

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random
import org.json4s._
import org.json4s.jackson.JsonMethods._


object SingleProducerToOnePartition extends MetricsCalculator with MetadataCreator with BenchmarkBase {
  def main(args: Array[String]) {
    checkParams(args)
    val configFilePath = args(0)
    val resultDirectoryPath = args(1)

    // Read config
    implicit val formats = DefaultFormats

    val inputString: String = Source.fromFile(configFilePath).getLines().toList.mkString("")
    val parsed = parse(inputString)
    // constants
    val init = (parsed \\ "InitMetadata").extract[Boolean]
    val recordByteSize = (parsed \\ "RecordByteSize").extract[Int]
    val recordsPerTransaction = (parsed \\ "RecordsPerTransaction").extract[Int]
    val transactionsNumber = (parsed \\ "TransactionsNumber").extract[Int]
    val transactionsPerBatch = (parsed \\ "TransactionsPerBatch").extract[Int]
    val batchInsert = (parsed \\ "InsertType" \\ "batchInsert").extract[Boolean]
    val batchSize = if (batchInsert) (parsed \\ "InsertType" \\ "batchSize").extract[Int] else 1
    val dataStorageType = (parsed \\ "DataStorageType").extract[String]
    // cassandra
    val cassandraKeyspace = (parsed \\ "Cassandra" \\ "Keyspace").extract[String]
    val cassandraHosts = (parsed \\ "Cassandra" \\ "Hosts").extract[List[Map[String,Int]]].map{ x =>
      val hostAndPort: (String, Int) = x.toList.head
      new InetSocketAddress(hostAndPort._1, hostAndPort._2)
    }
    // aerospike
    val aerospikeNamespace = (parsed \\ "Aerospike" \\ "Namespace").extract[String]
    val aerospikeHosts = (parsed \\ "Aerospike" \\ "Hosts").extract[List[Map[String,Int]]].map{ x =>
      val hostAndPort: (String, Int) = x.toList.head
      new Host(hostAndPort._1, hostAndPort._2)
    }
    // redis
    val redisHosts = (parsed \\ "Redis" \\ "Hosts").extract[List[Map[String,Int]]]

    // Run it only once
    if (init)
      initMetadata(cassandraHosts, cassandraKeyspace)

    // Start benchmark
    val dataToSend = Random.alphanumeric.take(recordByteSize).mkString("")
    val bytes = dataToSend.getBytes()
    val metadataFactory = new MetadataStorageFactory
    val metadataInstance = metadataFactory.getInstance(cassandraHosts, cassandraKeyspace)

    val aerospikeStorageOptions = new AerospikeStorageOptions(
      namespace = aerospikeNamespace,
      hosts = aerospikeHosts)
    val cassandraStorageOptions = new CassandraStorageOptions(
      cassandraHosts,
      cassandraKeyspace
    )

    var cassandraStorageFactory : CassandraStorageFactory = null
    var aerospikeStorageFactory : AerospikeStorageFactory = null

    val storageInstance: IStorage[Array[Byte]] = dataStorageType match {
        case "aerospike"  =>
          aerospikeStorageFactory = new AerospikeStorageFactory
          aerospikeStorageFactory.getInstance(aerospikeStorageOptions)
        case "cassandra"  =>
          cassandraStorageFactory = new CassandraStorageFactory
          cassandraStorageFactory.getInstance(cassandraStorageOptions)
        case storage      => throw new IllegalArgumentException(s"Data storage '$storage' is not supported")
      }

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
      metadataStorage = metadataInstance,
      dataStorage = storageInstance,
      lockService = lockerFactory,
      ttl = 60 * 60 * 24,
      description = "some_description")

    val policy = new RoundRobinPolicy(stream, List(0))

    val stringToArrayByteConverter = new StringToArrayByteConverter
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 60,
      transactionKeepAliveInterval = 20,
      producerKeepAliveInterval = 1,
      writePolicy = policy,
      insertType = if (batchInsert) BatchInsert(batchSize) else SingleElementInsert,
      txnGenerator = new LocalTimeTxnGenerator(),
      stringToArrayByteConverter)

    val producer: BasicProducer[String, Array[Byte]] = new BasicProducer("some_producer_name", stream, producerOptions)

    var tstart = System.nanoTime()

    var timePerTransactionBatch = ListBuffer[Long]()

    (0 until transactionsNumber).foreach{ cur =>
      val transaction = producer.newTransaction(false)
      (0 until recordsPerTransaction).foreach{ _ =>
        transaction.send(dataToSend)
      }
      transaction.close()

      if (cur % transactionsPerBatch == transactionsPerBatch - 1) {
        val newTstart = System.nanoTime()
        val difference = (newTstart - tstart)/(1000 * 1000)
        tstart = newTstart
        timePerTransactionBatch += difference

        println("Time per transaction batch " + (cur / transactionsPerBatch + 1) + s" = $difference ms")
      }
    }

    // Calculate statistics data
    val avg = getMedian(timePerTransactionBatch.toList)
    val perc = getPercentile(timePerTransactionBatch.toList, 0.95)
    val variance = getVariance(timePerTransactionBatch.toList)
    val totalTime = getSum(timePerTransactionBatch.toList)

    val totalStatistics = Map(
      "transactionsNumber"    -> transactionsNumber,
      "recordsPerTransaction" -> recordsPerTransaction,
      "recordByteSize"        -> recordByteSize,
      "timeUnit"              -> "ms",
      "totalTime"             -> totalTime,
      "averageTime"           -> avg,
      "percentile95"          -> perc,
      "variance"              -> variance
    )
    case class asd(batchSize : Int, transactionBatchTiming : List[Long])
    val timingStatistics = asd(transactionsPerBatch, timePerTransactionBatch.toList)

    // Write statistics to files
    val file1 = new File(resultDirectoryPath + "Total_statistics.json")
    val file2 = new File(resultDirectoryPath + "Transactions_timing.json")
    val pw1 = new java.io.PrintWriter(file1)
    val pw2 = new java.io.PrintWriter(file2)
    pw1.write(JsonUtils.toPrettyJson(totalStatistics))
    pw1.close()
    pw2.write(JsonUtils.toPrettyJson(timingStatistics))
    pw2.close()

    metadataFactory.closeFactory()
    if (cassandraStorageFactory != null) {
      cassandraStorageFactory.closeFactory()
    }
    if (aerospikeStorageFactory != null) {
      aerospikeStorageFactory.closeFactory()
    }
    lockerFactory.closeFactory()
  }
}
