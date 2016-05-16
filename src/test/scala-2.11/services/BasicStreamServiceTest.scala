package services

import java.net.InetSocketAddress
import com.aerospike.client.Host
import com.bwsw.tstreams.coordination.Coordinator
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.services.BasicStreamService
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.redisson.{Redisson, Config}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringCreator}


class BasicStreamServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  def randomString: String = RandomStringCreator.randomAlphaString(10)
  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)

  val config = new Config()
  config.useSingleServer().setAddress("localhost:6379")
  val redissonClient = Redisson.create(config)
  val coordinator = new Coordinator("some_path", redissonClient)
  val dataStorageFactory = new AerospikeStorageFactory
  val metadataStorageFactory = new MetadataStorageFactory
  
  val hosts = List(
    new Host("localhost",3000),
    new Host("localhost",3001),
    new Host("localhost",3002),
    new Host("localhost",3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)

  val storageInst = dataStorageFactory.getInstance(aerospikeOptions)
  val metadataStorageInst = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)


  "BasicStreamService.createStream()" should "create stream" in {
    val name = randomString

    val stream: BasicStream[_] = BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst,
      coordinator = coordinator)

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
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst,
      coordinator = coordinator)

      BasicStreamService.createStream(
        streamName = name,
        partitions = 3,
        ttl = 100,
        description = "some_description",
        metadataStorage = metadataStorageInst,
        dataStorage = storageInst,
        coordinator = coordinator)
    }
  }

  "BasicStreamService.loadStream()" should "load created stream" in {
    val name = randomString

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst,
      coordinator = coordinator)

    val stream: BasicStream[_] = BasicStreamService.loadStream(name, metadataStorageInst, storageInst, coordinator)
    val checkVal = stream.isInstanceOf[BasicStream[_]]
    checkVal shouldBe true
  }

  "BasicStreamService.isExist()" should "say exist concrete stream or not" in {
    val name = randomString
    val notExistName = randomString

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst,
      coordinator = coordinator)

    val isExist = BasicStreamService.isExist(name, metadataStorageInst)
    val isNotExist = BasicStreamService.isExist(notExistName, metadataStorageInst)
    val checkVal = isExist && !isNotExist

    checkVal shouldBe true
  }

  "BasicStreamService.loadStream()" should "throw exception if stream not exist" in {
    val name = randomString

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataStorageInst, storageInst, coordinator)
    }
  }

  "BasicStreamService.deleteStream()" should "delete created stream" in {
    val name = randomString

    BasicStreamService.createStream(
      streamName = name,
      partitions = 3,
      ttl = 100,
      description = "some_description",
      metadataStorage = metadataStorageInst,
      dataStorage = storageInst,
      coordinator = coordinator)

    BasicStreamService.deleteStream(name, metadataStorageInst)

    intercept[IllegalArgumentException] {
      BasicStreamService.loadStream(name, metadataStorageInst, storageInst, coordinator)
    }
  }

  "BasicStreamService.deleteStream()" should "throw exception if stream was not created before" in {
    val name = randomString

    intercept[IllegalArgumentException] {
      BasicStreamService.deleteStream(name, metadataStorageInst)
    }
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
  }
}
