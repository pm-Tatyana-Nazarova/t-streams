package com.bwsw.tstreams.entities


import com.bwsw.tstreams.agents.group.{ProducerCommitInfo, ConsumerCommitInfo, CommitInfo}
import com.datastax.driver.core.{BatchStatement, Session}
import org.slf4j.LoggerFactory


class GroupCommitEntity(consumerEntityName: String, producerEntityName: String, session: Session) {

  /**
   * Group Commit Entity logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Statement for saving consumer single offset
   */
  private val consumerCommitStatement = session
    .prepare(s"insert into $consumerEntityName (name,stream,partition,last_transaction) values(?,?,?,?)")

  /**
   * Statement using for saving producer offsets
   */
  private val producerCommitStatement = session
    .prepare(s"insert into $producerEntityName (stream,partition,transaction,cnt) values(?,?,?,?) USING TTL ?")

  def groupCommit(info : List[CommitInfo]): Unit ={
    val batchStatement = new BatchStatement()

    info foreach {
      case ConsumerCommitInfo(name, stream, partition, offset) =>
          batchStatement.add(consumerCommitStatement.bind(name, stream, new Integer(partition), offset))
      case ProducerCommitInfo(streamName, partition, transaction, totalCnt, ttl) =>
          batchStatement.add(producerCommitStatement.bind(streamName, new Integer(partition), transaction, new Integer(totalCnt), new Integer(ttl)))
    }

    logger.debug(s"start executing group commit statement with size:{${info.size}}\n")
    session.execute(batchStatement)
    logger.debug(s"finished executing group commit statement with size:{${info.size}}\n")
  }
}
