package com.bwsw.tstreams.entities

import java.util
import java.util.{Comparator, UUID}
import com.datastax.driver.core.{Row, Session}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


/**
 * Transactions settings
 * @param time Time of transaction
 * @param totalItems Total packets in transaction
 */
case class TransactionSettings(time : UUID, totalItems : Int)

/**
 * Metadata entity for commits
 * @param commitLog Table name in C*
 * @param session Session to use for this entity
 */
class CommitEntity(commitLog : String, session: Session) {

  /**
   * Commit Entity logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Session prepared statement using for inserting info in metadata in commit log
   */
  private val commitStatement = session
    .prepare(s"insert into $commitLog (stream,partition,transaction,cnt) values(?,?,?,?) USING TTL ?")

  /**
   * Session prepared statement using for transactions selection from metadata from commit log
   */
  private val selectTransactionsMoreThanStatement = session
    .prepare(s"select transaction,cnt from $commitLog where stream=? AND partition=? AND transaction>? LIMIT ?")

  /**
   * Session prepared statement using for selecting last transaction
   */
  private val selectTransactionsLessThanStatement = session
    .prepare(s"select transaction,cnt from $commitLog where stream=? AND partition=? AND transaction<? LIMIT ?")

  /**
   * Session prepared statement using for transaction total amount selection from metadata from commit log
   */
  private val selectTransactionAmountStatement = session
    .prepare(s"select cnt from $commitLog where stream=? AND partition=? AND transaction=? LIMIT 1")
  
  /**
   * Opening/Closing some specific transaction
   * @param streamName name of the stream
   * @param partition number of partition
   * @param transaction transaction unique id
   * @param totalCnt total amount of pieces of data in concrete transaction
   * @param ttl time of transaction existence in seconds
   */
  def commit(streamName : String, partition : Int, transaction: UUID, totalCnt : Int, ttl : Int) : Unit = {
    logger.info(s"start inserting in commit table with stream : {$streamName}, partition: {$partition}, totalCnt: {$totalCnt}, ttl: {$ttl}\n")
    val values = List(streamName, new Integer(partition), transaction, new Integer(totalCnt), new Integer(ttl))
    val statementWithBindings = commitStatement.bind(values:_*)
    logger.debug(s"start executing executing commit insertion statement with" +
      s" stream : {$streamName}, partition: {$partition}, totalCnt: {$totalCnt}, ttl:{$ttl}\n")
    session.execute(statementWithBindings)
    logger.info(s"finished inserting in commit table with stream : {$streamName}, partition: {$partition}, totalCnt: {$totalCnt}, ttl: {$ttl}\n")
  }

  /**
   * Retrieving some amount of transactions
   * @param streamName Name of the stream
   * @param partition Number of the partition
   * @param lastTransaction Transaction from which start to retrieve
   * @param cnt Amount of retrieved queue (can be less than cnt in case of insufficiency of transactions)
   * @return Queue of selected transactions
   */
  def getTransactionsMoreThan(streamName : String, partition : Int, lastTransaction : UUID, cnt : Int) : scala.collection.mutable.Queue[TransactionSettings] = {
    logger.info(s"start retrieving transactions from commit table with stream : {$streamName}, partition: {$partition}\n")
    val values : List[AnyRef] = List(streamName, new Integer(partition), lastTransaction, new Integer(cnt))
    val statementWithBindings = selectTransactionsMoreThanStatement.bind(values:_*)

    logger.debug(s"start executing transactions retrieving statement with stream : {$streamName}, partition: {$partition}\n")
    val selected = session.execute(statementWithBindings)

    logger.info(s"finished retrieving transactions from commit table with stream : {$streamName}, partition: {$partition}\n")
    val q = scala.collection.mutable.Queue[TransactionSettings]()
    val it = selected.iterator()
    while(it.hasNext){
      val value = it.next()
      q.enqueue(TransactionSettings(value.getUUID("transaction"), value.getInt("cnt")))
    }
    q
  }


  def getTransactionsLessThan(streamName : String, partition : Int, lastTransaction : UUID, cnt : Int=128) : util.TreeSet[TransactionSettings] = {
    logger.info(s"start retrieving transactions from commit table with stream : {$streamName}, partition: {$partition}\n")
    val values : List[AnyRef] = List(streamName, new Integer(partition), lastTransaction, new Integer(cnt))
    val statementWithBindings = selectTransactionsLessThanStatement.bind(values:_*)

    logger.debug(s"start executing transactions retrieving statement with stream : {$streamName}, partition: {$partition}\n")
    val selected = session.execute(statementWithBindings)

    logger.info(s"finished retrieving transactions from commit table with stream : {$streamName}, partition: {$partition}\n")

    val tree = new util.TreeSet[TransactionSettings](new Comparator[TransactionSettings](){
      override def compare(first: TransactionSettings, second: TransactionSettings): Int = {
        val tsFirst = first.time.timestamp()
        val tsSecond = second.time.timestamp()
        if (tsFirst < tsSecond) 1
        else if (tsFirst > tsSecond) -1
        else 0
      }})

    val it = selected.iterator()
    while(it.hasNext){
      val value = it.next()
      tree.add(TransactionSettings(value.getUUID("transaction"), value.getInt("cnt")))
    }

    tree
  }


  /**
   * Retrieving only one concrete transaction
   * @param streamName Name of concrete stream
   * @param partition Number of partition
   * @param transaction Concrete transaction time
   * @return Amount of data in concrete transaction
   */
  def getTransactionAmount(streamName : String, partition : Int, transaction : UUID) : Option[Int] = {
    logger.info(s"start retrieving transaction amount from commit table with stream : {$streamName}, partition: {$partition}\n")
    val values : List[AnyRef] = List(streamName, new Integer(partition), transaction)
    val statementWithBindings = selectTransactionAmountStatement.bind(values:_*)

    logger.debug(s"start executing transaction amount retrieving statement with stream : {$streamName}, partition: {$partition}\n")
    val selected = session.execute(statementWithBindings)

    logger.info(s"finished retrieving transaction amount from commit table with stream : {$streamName}, partition: {$partition}\n")
    val list: util.List[Row] = selected.all()
    if (list.isEmpty)
      None
    else
      Some(list.get(0).getInt("cnt"))
  }
}
