package entities

import java.util.UUID

import com.bwsw.tstreams.entities.ConsumerEntity
import com.datastax.driver.core.{Session, Cluster}
import com.gilt.timeuuid.TimeUuid
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraEntities, RandomStringGen}


class ConsumerEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringGen.randomAlphaString(10)

  var randomKeyspace : String = null
  var temporaryCluster : Cluster = null
  var temporarySession: Session = null
  var connectedSession : Session = null

  override def beforeAll(): Unit = {
    randomKeyspace = randomString
    temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
    temporarySession = temporaryCluster.connect()

    CassandraEntities.createKeyspace(temporarySession, randomKeyspace)
    CassandraEntities.createMetadataTables(temporarySession, randomKeyspace)

    connectedSession = temporaryCluster.connect(randomKeyspace)
  }

  "ConsumerEntity.saveSingleOffset(); ConsumerEntity.exist(); ConsumerEntity.getOffset()" should "create new consumer with particular offset," +
    " then check consumer existence, then get this consumer offset" in {

    val consumerEntity = new ConsumerEntity("consumers", connectedSession)
    val consumer = randomString
    val stream = randomString
    val partition = 1
    val offset = TimeUuid()
    consumerEntity.saveSingleOffset(consumer, stream, partition, offset)
    val checkExist: Boolean = consumerEntity.exist(consumer)
    val retValOffset: UUID = consumerEntity.getOffset(consumer, stream, partition)

    assert(checkExist && retValOffset == offset)
  }

  "ConsumerEntity.exist()" should "return false if consumer not exist" in {
    val consumerEntity = new ConsumerEntity("consumers", connectedSession)
    val consumer = randomString
    consumerEntity.exist(consumer) shouldEqual false
  }

  "ConsumerEntity.getOffset()" should "throw java.lang.IndexOutOfBoundsException if consumer not exist" in {
    val consumerEntity = new ConsumerEntity("consumers", connectedSession)
    val consumer = randomString
    val stream = randomString
    val partition = 1
    intercept[java.lang.IndexOutOfBoundsException] {
      consumerEntity.getOffset(consumer,stream,partition)
    }
  }

  "ConsumerEntity.saveBatchOffset(); ConsumerEntity.getOffset()" should "create new consumer with particular offsets and " +
    "then validate this consumer offsets" in {

    val consumerEntity = new ConsumerEntity("consumers", connectedSession)
    val consumer = randomString
    val stream = randomString
    val offsets = scala.collection.mutable.Map[Int,UUID]()
    for (i <- 0 to 100)
      offsets(i) = TimeUuid()

    consumerEntity.saveBatchOffset(consumer,stream,offsets)

    for (i <- 0 to 100){
      val uuid: UUID = consumerEntity.getOffset(consumer, stream, i)
      uuid shouldEqual offsets(i)
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
