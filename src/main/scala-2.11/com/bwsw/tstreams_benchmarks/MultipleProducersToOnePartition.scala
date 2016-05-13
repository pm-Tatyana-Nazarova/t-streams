package com.bwsw.tstreams_benchmarks

import java.net.InetSocketAddress
import java.util.concurrent.{Callable, FutureTask, RunnableFuture}
import java.util.concurrent.locks.ReentrantLock

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.InsertionType.{BatchInsert, SingleElementInsert}
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
import com.bwsw.tstreams_benchmarks.utils.JsonUtils
import org.redisson.{Config, Redisson}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

/**
  * Multiple producers send data to one partition and stop once the given amount of transactions is complete.
  * Time is logged once a group of transactions is completed by one producer, not for a single transaction.
  *
  * Then the following metrics are counted:
  * 1) total time for all transactions;
  * 2) average time of the group of transactions;
  * 3) time variance of the group of transactions;
  * 4) 95th percentile for the groups of transactions.
  *
  * Total number of transactions and number of transactions per group are defined in the configuration file.
  */
object MultipleProducersToOnePartition extends BenchmarkBase with MetricsCalculator with MetadataCreator {

  var configFile: ConfigLimitedByTransactions = null

  /**
    * Calculates and saves benchmark statistics
    *
    * @param resultDirectoryPath - path to the folder where to store results
    * @param timePerTransactionGroup - list with raw benchmark timing data
    * @param tCommonStart - benchmark start time
    * @param tCommonEnd - benchmark end time
    */
  def calculateAndSaveStatistics(resultDirectoryPath: String, timePerTransactionGroup: List[Long], tCommonStart: Long,
                                 tCommonEnd: Long) {
    if (configFile == null)
      throw new IllegalArgumentException("Config file is not set")

    // Calculate statistics data
    val average = getMedian(timePerTransactionGroup)
    val percentile = getPercentile(timePerTransactionGroup, 0.95)
    val variance = getVariance(timePerTransactionGroup)
    val totalTime = (tCommonEnd - tCommonStart) / (1000 * 1000)

    case class statistics(producersNumber: Int,
                          transactionsNumber: Int,
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
      producersNumber = configFile.ProducersNumber,
      transactionsNumber = timePerTransactionGroup.size * configFile.TransactionsPerGroup,
      transactionsPerGroup = configFile.TransactionsPerGroup,
      recordsPerTransaction = configFile.RecordsPerTransaction,
      recordByteSize = configFile.RecordByteSize,
      timeUnit = "ms",
      totalTime = totalTime,
      averageTime = average,
      percentile95 = percentile,
      variance = variance
    )
    val timingStatistics = timing(timePerTransactionGroup.length, timePerTransactionGroup)

    // Write statistics to files
    saveStatistics(resultDirectoryPath, JsonUtils.toPrettyJson(totalStatistics), totalStatistics = true)
    saveStatistics(resultDirectoryPath, JsonUtils.toPrettyJson(timingStatistics))
  }

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
    configFile = jsonSerializer.deserialize[ConfigLimitedByTransactions](inputString)

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

    val dataToSend = Random.alphanumeric.take(configFile.RecordByteSize).mkString("")
    val timePerTransactionGroup = new ListBuffer[Long]
    val transactionLock = new ReentrantLock(true)

    // Auxiliary values to distribute transactions among producers
    val groupsNumber = configFile.TransactionsNumber / (configFile.ProducersNumber * configFile.TransactionsPerGroup)
    val groupsExtra = configFile.TransactionsNumber % (configFile.ProducersNumber * configFile.TransactionsPerGroup) /
      configFile.TransactionsPerGroup

    val threadList = new ListBuffer[Thread]
    val futureTaskList = new ListBuffer[RunnableFuture[Boolean]]
    (1 until configFile.ProducersNumber + 1).foreach { i =>
      val futureTask = new FutureTask[Boolean](new Callable[Boolean] {
        override def call(): Boolean = {
          // TODO: move metadataFactory out of thread once it becomes thread-safe
          val metadataFactory = new MetadataStorageFactory
          val metadataInstance = metadataFactory.getInstance(cassandraHosts, cassandraKeyspace)

          var cassandraStorageFactory: CassandraStorageFactory = null
          var aerospikeStorageFactory: AerospikeStorageFactory = null

          try {
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

            val producer = new BasicProducer(s"Producer_" + i, stream, producerOptions)
            val producerTransactionsNumber = (groupsNumber + (if (i <= groupsExtra) 1 else 0)) * configFile.TransactionsPerGroup
            println(s"${producer.name}: $producerTransactionsNumber transactions")

            var tStart = System.nanoTime()

            (0 until producerTransactionsNumber).foreach { cur =>
              val transaction = producer.newTransaction(ProducerPolicies.cancelIfOpen)
              (0 until configFile.RecordsPerTransaction).foreach { _ =>
                transaction.send(dataToSend)
              }
              transaction.checkpoint()

              if ((cur + 1) % configFile.TransactionsPerGroup == 0) {
                val newTstart = System.nanoTime()
                val difference = (newTstart - tStart) / (1000 * 1000)
                tStart = newTstart
                transactionLock.lock()
                timePerTransactionGroup += difference
                transactionLock.unlock()
                println(s"Time per transaction group (${producer.name}) = $difference ms")
              }
            }
          }
          catch {
            case e: Throwable =>
              e.printStackTrace()
              return false
          }
          finally {
            if (cassandraStorageFactory != null) {
              cassandraStorageFactory.closeFactory()
            }
            if (aerospikeStorageFactory != null) {
              aerospikeStorageFactory.closeFactory()
            }
            metadataFactory.closeFactory()
          }
          true
        }
      })
      futureTaskList += futureTask
      threadList += new Thread(futureTask)
    }

    val tCommonStart = System.nanoTime()
    threadList.foreach { t => t.start() }
    threadList.foreach { t => t.join() }
    val tCommonEnd = System.nanoTime()

    client.shutdown()

    // Check that all threads have finished successfully
    futureTaskList.foreach { f => if (!f.get()) System.exit(1)}

    // Save statistics data
    calculateAndSaveStatistics(resultDirectoryPath, timePerTransactionGroup.toList, tCommonStart, tCommonEnd)
  }
}
