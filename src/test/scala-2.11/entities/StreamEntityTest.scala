package entities

import com.datastax.driver.core.{Session, Cluster}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringGen}
import com.bwsw.tstreams.entities.StreamEntity


class StreamEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)

  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var connectedSession : Session = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()

    CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
    CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)

    connectedSession = temporaryCluster.connect(randomKeyspace)
  }

  "StreamEntity.createStream(); StreamEntity.getStream()" should "create and retrieve created stream from metadata tables" in {
    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomString
    val partitions = 3
    val ttl = 3
    val description = randomString
    streamEntity.createStream(streamName, partitions, ttl, description)
    val streamSettings = streamEntity.getStream(streamName).get

    assert(streamSettings.name == streamName
      && streamSettings.partitions == partitions
      && streamSettings.description == description
      && streamSettings.ttl == ttl)
  }

  "StreamEntity.createStream(); StreamEntity.alternate(); StreamEntity.getStream()" should
    "create, alternate and retrieve created stream from metadata tables" in {

    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomString
    val partitions = 3
    val ttl = 3
    val description = randomString

    streamEntity.createStream(streamName, partitions, ttl, description)
    streamEntity.alternateStream(streamName, partitions+1, ttl+1, description+"a")

    val streamSettings = streamEntity.getStream(streamName).get

    assert(streamSettings.name == streamName
      && streamSettings.partitions == partitions+1
      && streamSettings.description == description+"a"
      && streamSettings.ttl == ttl+1)
  }

  "StreamEntity.createStream(); StreamEntity.delete(); StreamEntity.isExist()" should
    "create, delete and checking existence of stream from metadata tables" in {

    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomString
    val partitions = 3
    val ttl = 3
    val description = randomString

    streamEntity.createStream(streamName, partitions, ttl, description)
    streamEntity.deleteStream(streamName)
    streamEntity.isExist(streamName) shouldEqual false
  }


  "StreamEntity.createStream(); StreamEntity.createStream()" should
    "throw exception because two streams with equivalent name can't exist" in {

    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomString
    val partitions = 3
    val ttl = 3
    val description = randomString

    streamEntity.createStream(streamName, partitions, ttl, description)
    intercept[IllegalArgumentException] {
      streamEntity.createStream(streamName, partitions, ttl, description)
    }
  }

  "StreamEntity.alternateStream()" should
    "throw exception on non existing stream" in {

    val streamEntity = new StreamEntity("streams", connectedSession)
    val streamName = randomString
    val partitions = 3
    val ttl = 3
    val description = randomString
    intercept[IllegalArgumentException] {
      streamEntity.alternateStream(streamName, partitions, ttl, description)
    }
  }

  override def afterAll(): Unit = {
    val newCluster = Cluster.builder().addContactPoint("localhost").build()
    val newSession: Session = newCluster.connect()
    newSession.execute(s"DROP KEYSPACE $randomKeyspace")
    newCluster.close()
    newSession.close()

    if (!connectedSession.isClosed)
      connectedSession.close()
    if (!temporarySession.isClosed)
      temporarySession.close()
    if(!temporaryCluster.isClosed)
      temporaryCluster.close()
  }
}
