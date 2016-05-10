package com.bwsw.tstreams_benchmarks

import java.io.File
import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.InsertionType.{BatchInsert, SingleElementInsert}
import com.bwsw.tstreams_benchmarks.utils.JsonUtils
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerPolicies}
import com.bwsw.tstreams.common.JsonSerializer
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.RoundRobinPolicy
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams_benchmarks.config_classes.ConfigLimitedByTransactions
import org.redisson.{Config, Redisson}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

/**
  * Producer sends data to one partition and stops once the given amount of transactions is complete.
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

    val inputString: String = Source.fromFile(configFilePath).getLines().toList.mkString("")
    val jsonSerializer = new JsonSerializer
    val configFile: ConfigLimitedByTransactions = jsonSerializer.deserialize[ConfigLimitedByTransactions](inputString)

    // cassandra
    val cassandraKeyspace = configFile.Cassandra("Keyspace").asInstanceOf[String]
    val cassandraHosts = configFile.Cassandra("Hosts").asInstanceOf[List[Map[String, Int]]].map { x =>
      val hostAndPort: (String, Int) = x.toList.head
      new InetSocketAddress(hostAndPort._1, hostAndPort._2)
    }
    // redis
    val redisHosts = configFile.Redis("Hosts").asInstanceOf[List[Map[String, Int]]]

    // Run it only once
    if (configFile.InitMetadata)
      initMetadata(cassandraHosts, cassandraKeyspace)

    // Start benchmark
    val dataToSend = Random.alphanumeric.take(configFile.RecordByteSize).mkString("")
    val metadataFactory = new MetadataStorageFactory
    val metadataInstance = metadataFactory.getInstance(cassandraHosts, cassandraKeyspace)

    var cassandraStorageFactory: CassandraStorageFactory = null
    var aerospikeStorageFactory: AerospikeStorageFactory = null

    val storageInstance: IStorage[Array[Byte]] = configFile.DataStorageType match {
      case "aerospike" =>
        val aerospikeNamespace = configFile.Aerospike("Namespace").asInstanceOf[String]
        val aerospikeHosts = configFile.Aerospike("Hosts").asInstanceOf[List[Map[String, Int]]].map { x =>
          val hostAndPort: (String, Int) = x.toList.head
          new Host(hostAndPort._1, hostAndPort._2)
        }
        val aerospikeStorageOptions = new AerospikeStorageOptions(
          namespace = aerospikeNamespace,
          hosts = aerospikeHosts)
        aerospikeStorageFactory = new AerospikeStorageFactory
        aerospikeStorageFactory.getInstance(aerospikeStorageOptions)
      case "cassandra" =>
        val cassandraStorageOptions = new CassandraStorageOptions(
          cassandraHosts,
          cassandraKeyspace
        )
        cassandraStorageFactory = new CassandraStorageFactory
        cassandraStorageFactory.getInstance(cassandraStorageOptions)
      case storage => throw new IllegalArgumentException(s"Data storage '$storage' is not supported")
    }

    val convertedRedisHosts = redisHosts.map { x =>
      val hostAndPort = x.toList.head
      s"${hostAndPort._1}:${hostAndPort._2}"
    }
    val redisConfig = new Config()
    if (redisHosts.size == 1) {
      redisConfig.useSingleServer().setAddress(convertedRedisHosts.head)
    }
    else {
      //TODO not tested
      val masterName = configFile.Redis("MasterName").asInstanceOf[String]
      redisConfig
        .useSentinelServers()
        .setMasterName(masterName)
        .addSentinelAddress(convertedRedisHosts: _*)
    }

    val client = Redisson.create(redisConfig)
    val coordinator = new Coordinator("/prefix", client)

    val stream = new BasicStream[Array[Byte]](
      name = "benchmark_stream",
      partitions = 1,
      metadataStorage = metadataInstance,
      dataStorage = storageInstance,
      coordinator = coordinator,
      ttl = configFile.TTL,
      description = "some_description")

    val policy = new RoundRobinPolicy(stream, List(0))

    val batchInsert = configFile.InsertType("batchInsert").asInstanceOf[Boolean]
    val batchSize = if (batchInsert) configFile.InsertType("batchSize").asInstanceOf[Int] else 1
    val stringToArrayByteConverter = new StringToArrayByteConverter

    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 60,
      transactionKeepAliveInterval = 20,
      producerKeepAliveInterval = 1,
      writePolicy = policy,
      insertType = if (batchInsert) BatchInsert(batchSize) else SingleElementInsert,
      txnGenerator = new LocalTimeUUIDGenerator(),
      stringToArrayByteConverter)

    val producer: BasicProducer[String, Array[Byte]] = new BasicProducer("some_producer_name", stream, producerOptions)

    var tStart = System.nanoTime()

    var timePerTransactionGroup = ListBuffer[Long]()

    (0 until configFile.TransactionsNumber).foreach { cur =>
      val transaction = producer.newTransaction(ProducerPolicies.cancelIfOpen)
      (0 until configFile.RecordsPerTransaction).foreach { _ =>
        transaction.send(dataToSend)
      }
      transaction.checkpoint()

      if (cur % configFile.TransactionsPerGroup == configFile.TransactionsPerGroup - 1) {
        val newTstart = System.nanoTime()
        val difference = (newTstart - tStart) / (1000 * 1000)
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

    case class statistics(transactionsNumber: Int,
                          transactionsPerGroup: Int,
                          recordsPerTransaction: Int,
                          recordByteSize: Int,
                          timeUnit: String,
                          totalTime: Long,
                          averageTime: Double,
                          percentile95: Long,
                          variance: Double
                         )
    case class timing(groupsCount: Int, transactionGroupTiming: List[Long])

    val totalStatistics = statistics(
      timePerTransactionGroup.size * configFile.TransactionsPerGroup,
      configFile.TransactionsPerGroup,
      configFile.RecordsPerTransaction,
      configFile.RecordByteSize,
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
    client.shutdown()
  }
}
