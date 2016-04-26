package data

import java.util.UUID
import com.bwsw.tstreams.data.cassandra.CassandraStorage
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringCreator}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps


class CassandraStorageTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  def randomString: String = RandomStringCreator.randomAlphaString(10)
  val randomKeyspace = randomString
  var cluster = Cluster.builder().addContactPoint("localhost").build()
  var session = cluster.connect()
  CassandraHelper.createKeyspace(session,randomKeyspace)
  CassandraHelper.createDataTable(session,randomKeyspace)
  var connectedSession = cluster.connect(randomKeyspace)

  "CassandraStorage.init(), CassandraStorage.truncate() and CassandraStorage.remove()" should "create, truncate and remove data table" in {
    val cassandraStorage = new CassandraStorage(
      cluster = cluster,
      session = connectedSession,
      keyspace = randomKeyspace)

    var checkVal = true

    try {
      //here we already have created tables for entitiesRepository
      cassandraStorage.remove()
      cassandraStorage.init()
      cassandraStorage.truncate()
    }
    catch{
      case e : Exception => checkVal = false
    }

    checkVal shouldEqual true
  }

  "CassandraStorage.put() CassandraStorage.get()" should "insert data in cassandra storage and retrieve it" in {
    val cassandraStorage = new CassandraStorage(
      cluster = cluster,
      session = connectedSession,
      keyspace = randomKeyspace)

    val streamName: String = "stream_name"
    val partition: Int = 0
    val transaction: UUID = UUIDs.timeBased()
    val data = "some_data"
    val cnt = 1000

    val jobs = ListBuffer[() => Unit]()

    for (i <- 0 until 1000) {
      val future = cassandraStorage.put(streamName, partition, transaction, 60*60*24, data.getBytes, i)
      jobs += future
    }

    jobs.foreach(x=> x())

    val queue: mutable.Queue[Array[Byte]] = cassandraStorage.get(streamName, partition, transaction, 0, cnt-1)
    val emptyQueueForLeftBound = cassandraStorage.get(streamName, partition, transaction, -100, -1)
    val emptyQueueForRightBound = cassandraStorage.get(streamName, partition, transaction, cnt, cnt + 100)

    var checkVal = true

    if (emptyQueueForLeftBound.nonEmpty || emptyQueueForRightBound.nonEmpty)
      checkVal = false

    while(queue.nonEmpty){
      val part: String = new String(queue.dequeue())
      if (part != data)
        checkVal = false
    }

    checkVal shouldEqual true
  }


  override def afterAll() : Unit = {
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    cluster.close()
    session.close()
    connectedSession.close()
  }
}