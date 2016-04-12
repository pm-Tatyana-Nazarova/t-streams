package services

import java.net.InetSocketAddress
import com.aerospike.client.Host
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.lockservice.impl.RedisLockerFactory
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.redisson.Config
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringGen}


class BasicStreamServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  def randomString: String = RandomStringGen.randomAlphaString(10)
  val config = new Config()
  config.useSingleServer().setAddress("localhost:6379")
  val lockerFactory = new RedisLockerFactory("some_path/", config)
  val dataFactory = new AerospikeStorageFactory
  val metadataFactory = new MetadataStorageFactory

  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()

  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)

  val hosts = List(
    new Host("localhost",3000),
    new Host("localhost",3001),
    new Host("localhost",3002),
    new Host("localhost",3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)

  val storageInst = dataFactory.getInstance(aerospikeOptions)
  val metadataInst = metadataFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)


  "BasicStreamService.createStream()" should "create stream" in {
    val name = randomString

    val stream: BasicStream[_] = BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataInst,
      dataStorage = storageInst,
      lockService = lockerFactory)

    val checkVal = stream.isInstanceOf[BasicStream[_]]

    checkVal shouldBe true
  }

  "BasicStreamService.createStream()" should "throw exception if stream already created" in {
    intercept[IllegalArgumentException] {
      val name = randomString

      BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataInst,
      dataStorage = storageInst,
      lockService = lockerFactory)

      BasicStreamService.createStream(
        streamName = name,
        partitions = 3,
        ttl = 100,
        description = "some_description",
        metadataStorage = metadataInst,
        dataStorage = storageInst,
        lockService = lockerFactory)
    }
  }

  "BasicStreamService.loadStream()" should "load created stream" in {
    val name = randomString

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataInst,
      dataStorage = storageInst,
      lockService = lockerFactory)

    val stream: BasicStream[_] = BasicStreamService.loadStream(name, metadataInst, storageInst, lockerFactory)
    val checkVal = stream.isInstanceOf[BasicStream[_]]
    checkVal shouldBe true
  }

  "BasicStreamService.loadStream()" should "throw exception if stream not exist" in {
    val name = randomString

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataInst, storageInst, lockerFactory)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = randomString

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataInst,
      dataStorage = storageInst,
      lockService = lockerFactory)

    BasicStreamService.deleteStream(name, metadataInst)

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataInst, storageInst, lockerFactory)
    }
  }

  "BasicStreamService.deleteStream()" should "throw exception if stream was not created before" in {
    val name = randomString

    intercept[IllegalArgumentException] {
      BasicStreamService.deleteStream(name, metadataInst)
    }
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
