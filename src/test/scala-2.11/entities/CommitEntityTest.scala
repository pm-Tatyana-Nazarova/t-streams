package entities

import com.bwsw.tstreams.entities.{TransactionSettings, CommitEntity}
import com.datastax.driver.core.{Session, Cluster}
import com.gilt.timeuuid.TimeUuid
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{CassandraHelper, RandomStringGen}

import scala.collection.mutable


class CommitEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
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

  "CommitEntity.commit() CommitEntity.getTransactionAmount()" should
    "commit - add new commit in commit metadata table and it should expire after some time(ttl param)," +
    "getTransactionAmount - retrieve this transaction amount from commit_log" in {

    val commitEntity = new CommitEntity("commit_log", connectedSession)
    val stream = randomString
    val txn = TimeUuid()
    val partition = 10
    val totalCnt = 123
    val ttl = 3

    commitEntity.commit(stream, partition, txn, totalCnt, ttl)

    //read all transactions from oldest offset
    val amount: Int = commitEntity.getTransactionAmount(stream, partition, txn).get

    assert(amount == totalCnt)
    Thread.sleep(3000)

    val emptyQueue: mutable.Queue[TransactionSettings] = commitEntity.getTransactions(stream, partition, TimeUuid(0), 1)
    assert(emptyQueue.isEmpty)
  }

  "CommitEntity.commit() CommitEntity.getTransactions()" should
    "commit - add new commit in commit metadata table" +
      "getTransactions - retrieve this transactions from commit_log" in {

    val commitEntity = new CommitEntity("commit_log", connectedSession)
    val stream = randomString
    val txn1 = TimeUuid(1)
    val txn2 = TimeUuid(2)
    val partition = 10
    val totalCnt = 123
    val ttl = 3

    commitEntity.commit(stream, partition, txn1, totalCnt, ttl)
    commitEntity.commit(stream, partition, txn2, totalCnt, ttl)

    val queue = commitEntity.getTransactions(stream, partition, TimeUuid(0), 2)
    assert(queue.size == 2)
    val txnSettings1 = queue.dequeue()
    assert(txnSettings1.time == txn1 && txnSettings1.totalItems == totalCnt)

    val txnSettings2 = queue.dequeue()
    assert(txnSettings2.time == txn2 && txnSettings2.totalItems == totalCnt)
  }

  override def afterAll(): Unit = {
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    connectedSession.close()
    temporarySession.close()
    temporaryCluster.close()
  }
}
