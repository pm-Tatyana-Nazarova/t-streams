package mastervoting

import java.net.InetSocketAddress
import java.util.logging.LogManager

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.producer.InsertionType.BatchInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, PeerToPeerAgentSettings}
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator
import com.bwsw.tstreams.interaction.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.policy.RoundRobinPolicy
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.redisson.{Config, Redisson}
import testutils.CassandraHelper


object BasicProducerTest{
  def main(args: Array[String]) {
//    agentaddress, zk{host:port}, cassandra{host:port}, aerospike{host:port;host:port}, redis{host:port}
    LogManager.getLogManager().reset()
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    System.setProperty("org.slf4j.simpleLogger.logFile","log/testlog.log")
    System.setProperty("org.slf4j.simpleLogger.showDateTime","false")
    System.setProperty("org.slf4j.simpleLogger.log.com.bwsw","DEBUG")

    val agentSettings = new PeerToPeerAgentSettings(
      agentAddress = "localhost:8888",
      zkHosts = List(new InetSocketAddress("localhost", 2181)),
      zkRootPath = "/unit",
      zkTimeout = 7000,
      isLowPriorityToBeMaster = false,
      transport = new TcpTransport,
      transportTimeout = 5)

    val randomKeyspace = "test"

    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    try {
      CassandraHelper.createKeyspace(session, randomKeyspace)
      CassandraHelper.createMetadataTables(session, randomKeyspace)
    }
    catch {
      case e : Exception =>
    }

    //metadata/data factories
    val metadataStorageFactory = new MetadataStorageFactory
    val storageFactory = new AerospikeStorageFactory

    //converters to convert usertype->storagetype; storagetype->usertype
    val stringToArrayByteConverter = new StringToArrayByteConverter

    //aerospike storage instances
    val hosts = List(
      new Host("localhost",3000),
      new Host("localhost",3001),
      new Host("localhost",3002),
      new Host("localhost",3003))
    val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
    val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)

    //metadata storage instances
    val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
      cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
      keyspace = randomKeyspace)

    //coordinator for coordinating producer/consumer
    val config = new Config()
    config.useSingleServer().setAddress("localhost:6379")
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

    producer.agent.stop()
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    redissonClient.shutdown()
  }
}