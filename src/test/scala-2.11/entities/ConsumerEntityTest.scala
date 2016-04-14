package entities

import java.util.UUID
import com.bwsw.tstreams.entities.ConsumerEntity
import com.datastax.driver.core.Cluster
import com.gilt.timeuuid.TimeUuid
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringCreator}


class ConsumerEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringCreator.randomAlphaString(10)

  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  val connectedSession = temporaryCluster.connect(randomKeyspace)

  "ConsumerEntity.saveSingleOffset() ConsumerEntity.exist() ConsumerEntity.getOffset()" should "create new consumer with particular offset," +
    " then check consumer existence, then get this consumer offset" in {

    val consumerEntity = new ConsumerEntity("consumers", connectedSession)
    val consumer = randomString
    val stream = randomString
    val partition = 1
    val offset = TimeUuid()
    consumerEntity.saveSingleOffset(consumer, stream, partition, offset)
    val checkExist: Boolean = consumerEntity.exist(consumer)
    val retValOffset: UUID = consumerEntity.getOffset(consumer, stream, partition)

    val checkVal = checkExist && retValOffset == offset
    checkVal shouldBe true
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

    var checkVal = true

    for (i <- 0 to 100){
      val uuid: UUID = consumerEntity.getOffset(consumer, stream, i)
      checkVal &= uuid == offsets(i)
    }
    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    connectedSession.close()
    temporarySession.close()
    temporaryCluster.close()
  }
}
