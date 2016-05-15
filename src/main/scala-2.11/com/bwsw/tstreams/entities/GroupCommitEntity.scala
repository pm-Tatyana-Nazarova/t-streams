package com.bwsw.tstreams.entities


import com.bwsw.tstreams.agents.group.{ProducerCommitInfo, ConsumerCommitInfo, CommitInfo}
import com.datastax.driver.core.{BatchStatement, Session}


class GroupCommitEntity(consumerEntityName: String, producerEntityName: String, session: Session) {
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
    session.execute(batchStatement)
  }
}
