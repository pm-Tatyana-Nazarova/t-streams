package com.bwsw.tstreams.entities

import java.util.UUID
import com.datastax.driver.core.{Session, BatchStatement}
import org.slf4j.LoggerFactory

/**
 * Consumer entity for interact with consumers metadata
 * @param entityName Metadata table name
 * @param session Session with metadata
 */
class ConsumerEntity(entityName : String, session : Session) {

  /**
   * Consumer Entity logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Statement for check exist or not some specific consumer
   */
  private val existStatement = session
     .prepare(s"select name from $entityName where name=? limit 1")

  /**
   * Statement for saving single offset
   */
  private val saveSingleOffsetStatement = session
      .prepare(s"insert into $entityName (name,stream,partition,last_transaction) values(?,?,?,?)")

  /**
   * Statement for retrieving offsets from consumers metadata
   */
  private val getOffsetStatement = session
    .prepare(s"select last_transaction from $entityName where name=? AND stream=? AND partition=? limit 1")


  /**
   * Checking exist or not concrete consumer
   * @param consumerName Name of the consumer
   * @return Exist or not concrete consumer
   */
  def exist(consumerName : String) : Boolean = {
    logger.info(s"start checking consumer for existence with name : {$consumerName}\n")
    val statementWithBindings = existStatement.bind(consumerName)

    logger.debug(s"start executing statement for existence check with name : {$consumerName}\n")
    val res = session.execute(statementWithBindings).all()

    logger.info(s"finished checking consumer for existence with name : {$consumerName}\n")
    !res.isEmpty
  }

  /**
   * Saving offset batch
   * @param name Name of the consumer
   * @param stream Name of the specific stream
   * @param partitionAndLastTxn Set of partition and last transaction pairs to save
   */
  def saveBatchOffset(name : String, stream: String, partitionAndLastTxn : scala.collection.mutable.Map[Int, UUID]) : Unit = {
    logger.info(s"start inserting batch offset in stream : {$stream} with consumer : {$name}\n")

    val batchStatement = new BatchStatement()
    partitionAndLastTxn.map{ case(partition,lastTxn) =>
      val values : List[AnyRef] = List(name,stream,new Integer(partition),lastTxn)
      val statementWithBindings = saveSingleOffsetStatement.bind(values:_*)
      batchStatement.add(statementWithBindings)
    }

    logger.debug(s"start executing insertion batch offset statement in stream : {$stream} with consumer : {$name}\n")
    session.execute(batchStatement)

    logger.info(s"finished inserting batch offset in stream : {$stream} with consumer : {$name}\n")
  }

  /**
   * Saving single offset
   * @param name Name of the specific consumer
   * @param stream Name of the specific stream
   * @param partition Name of the specific partition
   * @param offset Offset to save
   */
  def saveSingleOffset(name : String, stream : String, partition : Int, offset : UUID) : Unit = {
    logger.info(s"start inserting single offset in stream : {$stream}, partition : {$partition} with consumer : {$name}\n")

    val values : List[AnyRef] = List(name,stream,new Integer(partition),offset)
    val statementWithBindings = saveSingleOffsetStatement.bind(values:_*)
    logger.debug(s"start executing insertion single offset statement in stream : {$stream}, partition : {$partition} with consumer : {$name}\n")
    session.execute(statementWithBindings)

    logger.info(s"finished single offset in stream : {$stream}, partition : {$partition} with consumer : {$name}\n")
  }

  /**
   * Retrieving specific offset for particular consumer
   * @param name Name of the specific consumer
   * @param stream Name of the specific stream
   * @param partition Name of the specific partition
   * @return Offset
   */
  def getOffset(name : String, stream : String, partition : Int) : UUID = {
    logger.info(s"start getting single offset in stream : {$stream}, partition : {$partition} with consumer : {$name}\n")

    val values = List(name, stream, new Integer(partition))
    val statementWithBindings = getOffsetStatement.bind(values:_*)
    logger.debug(s"start executing selecting single offset statement in stream : {$stream}, partition : {$partition} with consumer : {$name}\n")
    val selected = session.execute(statementWithBindings).all()

    logger.info(s"finished getting single offset in stream : {$stream}, partition : {$partition} with consumer : {$name}\n")
    selected.get(0).getUUID("last_transaction")
  }
}
