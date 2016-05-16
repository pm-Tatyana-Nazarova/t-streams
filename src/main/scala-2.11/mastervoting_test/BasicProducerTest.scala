package mastervoting_test

import java.net.InetSocketAddress
import java.util.logging.LogManager

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{ProducerPolicies, BasicProducer, BasicProducerOptions, PeerToPeerAgentSettings}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import com.bwsw.tstreams.interaction.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.RoundRobinPolicy
import com.bwsw.tstreams.streams.BasicStream
import org.redisson.{Config, Redisson}


object BasicProducerTest{
  def main(args: Array[String]) {
    if (args.length != 6){
      throw new IllegalArgumentException("usage: [cnt] [agentAddress] [zk{host:port}], [cassandra{host:port}], [aerospike{host:port}], [redis{host:port}]")
    }
    assert(args.length == 6)
    val cnt = args(0).toInt
    val agentAddress = args(1)
    val zkHosts = args(2).split(";").map{x=>
      val hp = x.split(":")
      val (host,port) = (hp(0),hp(1))
      new InetSocketAddress(host,port.toInt)
    }
    val cassandraHosts = args(3).split(";").map{x=>
      val hp = x.split(":")
      val (host,port) = (hp(0),hp(1))
      new InetSocketAddress(host,port.toInt)
    }
    val aerospikeHosts = args(4).split(";").map{x=>
      val hp = x.split(":")
      val (host,port) = (hp(0),hp(1))
      new Host(host,port.toInt)
    }
    val redisHost = args(5)

    LogManager.getLogManager.reset()
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    System.setProperty("org.slf4j.simpleLogger.logFile","log/testlog.log")
    System.setProperty("org.slf4j.simpleLogger.showDateTime","false")
    System.setProperty("org.slf4j.simpleLogger.log.com.bwsw","DEBUG")

    val agentSettings = new PeerToPeerAgentSettings(
      agentAddress = agentAddress,
      zkHosts = zkHosts.toList,
      zkRootPath = "/unit",
      zkTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5)

    val randomKeyspace = "test"

    //metadata/data factories
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new AerospikeStorageFactory

    //converters to convert usertype->storagetype; storagetype->usertype
    val stringToArrayByteConverter = new StringToArrayByteConverter

    //aerospike storage instances
    val aerospikeOptions = new AerospikeStorageOptions("test", aerospikeHosts.toList)
    val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)

    //metadata storage instances
    val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
      cassandraHosts = cassandraHosts.toList,
      keyspace = randomKeyspace)

    //coordinator for coordinating producer/consumer
    val config = new Config()
    config.useSingleServer().setAddress(redisHost)
    val redissonClient = Redisson.create(config)
    val coordinator = new Coordinator("some_path", redissonClient)

    //stream instances for producer/consumer
    val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 1,
      metadataStorage = metadataStorageInstForProducer,
      dataStorage = aerospikeInstForProducer,
      coordinator = coordinator,
      ttl = 60 * 10,
      description = "some_description")

    //producer/consumer options
    val producerOptions = new BasicProducerOptions[String, Array[Byte]](
      transactionTTL = 6,
      transactionKeepAliveInterval = 2,
      producerKeepAliveInterval = 1,
      new RoundRobinPolicy(streamForProducer, List(0)),
      BatchInsert(10),
      new LocalTimeUUIDGenerator,
      agentSettings,
      stringToArrayByteConverter)

    val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)

    0 until cnt foreach { _ =>
      val txn = producer.newTransaction(ProducerPolicies.errorIfOpen)
      txn.send("info")
      txn.checkpoint()
      Thread.sleep(2000)
    }

    producer.agent.stop()
    metadataStorageFactory.closeFactory()
    redissonClient.shutdown()
  }
}