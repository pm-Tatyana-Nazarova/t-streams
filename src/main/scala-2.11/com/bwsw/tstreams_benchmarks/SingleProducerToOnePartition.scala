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

/**
  * Producer sends data to one partition and stops once the given value of transactions is complete.
  * Time is logged for each group of transactions, not for the single transaction.
  *
  * Then the following metrics are counted:
  * 1) total time for all transactions;
  * 2) average time of the group of transactions;
  * 3) time variance of the group of transactions;
  * 4) 95th percentile for the groups of transactions.
  *
  * Total number of transactions and number of transactions per group are defined in the configuration file.
  */
object SingleProducerToOnePartition extends MetricsCalculator with MetadataCreator with BenchmarkBase {
  /**
    * @param args 1) Path to configuration file
    *             2) Path to result folder
    */
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
    val transactionsPerGroup = (parsed \\ "TransactionsPerGroup").extract[Int]
    val batchInsert = (parsed \\ "InsertType" \\ "batchInsert").extract[Boolean]
    val batchSize = if (batchInsert) (parsed \\ "InsertType" \\ "batchSize").extract[Int] else 1
    val dataStorageType = (parsed \\ "DataStorageType").extract[String]
    val streamTTL = (parsed \\ "TTL").extract[Int]
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
      ttl = streamTTL,
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

    var tStart = System.nanoTime()

    var timePerTransactionGroup = ListBuffer[Long]()

    (0 until transactionsNumber).foreach{ cur =>
      val transaction = producer.newTransaction(false)
      (0 until recordsPerTransaction).foreach{ _ =>
        transaction.send(dataToSend)
      }
      transaction.close()

      if (cur % transactionsPerGroup == transactionsPerGroup - 1) {
        val newTstart = System.nanoTime()
        val difference = (newTstart - tStart)/(1000 * 1000)
        tStart = newTstart
        timePerTransactionGroup += difference

        println(s"Time per transaction group = $difference ms")
      }
    }

    // Calculate statistics data
    val average = getMedian(timePerTransactionGroup.toList)
    val percentile = getPercentile(timePerTransactionGroup.toList, 0.95)
    val variance = getVariance(timePerTransactionGroup.toList)
    val totalTime = getSum(timePerTransactionGroup.toList)

    case class statistics( transactionsNumber  : Int,
                           transactionsPerGroup : Int,
                           recordsPerTransaction : Int,
                           recordByteSize: Int,
                           timeUnit: String,
                           totalTime: Long,
                           averageTime: Double,
                           percentile95: Long,
                           variance: Double
                          )
    case class timing(groupsCount : Int, transactionGroupTiming : List[Long])

    val totalStatistics = statistics(
      transactionsNumber,
      transactionsPerGroup,
      recordsPerTransaction,
      recordByteSize,
      "ms",
      totalTime,
      average,
      percentile,
      variance
    )
    val timingStatistics = timing(timePerTransactionGroup.length, timePerTransactionGroup.toList)

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
