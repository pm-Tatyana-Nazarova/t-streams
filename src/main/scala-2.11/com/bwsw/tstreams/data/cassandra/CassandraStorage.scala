package com.bwsw.tstreams.data.cassandra

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.bwsw.tstreams.data.IStorage
import com.datastax.driver.core._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Cassandra storage impl of IStorage
 */
class CassandraStorage(cluster: Cluster, session: Session, keyspace: String) extends IStorage[Array[Byte]]{

  /**
   * CassandraStorage logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Prepared C* statement for data insertion
   */
  private val insertStatement = session
    .prepare(s"insert into data_queue (stream,partition,transaction,seq,data) values(?,?,?,?,?) USING TTL ?")

  /**
   * Prepared C* statement for select queries
   */
  private val selectStatement = session
    .prepare(s"select data from data_queue where stream=? AND partition=? AND transaction=? AND seq>=? AND seq<=? LIMIT ?")


  /**
   * Put data in the cassandra storage
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param data Data which will be put
   * @param partNum Data unique part number
   * @return Wait future (if insertion was not async wait future will complete instantly)
   */
  override def put(streamName : String,
                   partition : Int,
                   transaction: UUID,
                   ttl : Int,
                   data: Array[Byte],
                   partNum: Int) : Future[Unit] = {

    val values = List(streamName, new Integer(partition), transaction, new Integer(partNum), ByteBuffer.wrap(data), new Integer(ttl))

    val statementWithBindings = insertStatement.bind(values:_*)

    logger.debug(s"start inserting data for stream:{$streamName}, partition:{$partition}, partNum:{$partNum}\n")
    val res: ResultSetFuture = session
      .executeAsync(statementWithBindings)
    logger.debug(s"finished inserting data for stream:{$streamName}, partition:{$partition}, partNum:{$partNum}\n")

    val job: Future[Unit] = Future {
      res.getUninterruptibly
    }
    job
  }

  /**
   * Get data from cassandra storage
   * @param streamName Name of the stream
   * @param partition Number of stream partitions
   * @param transaction Number of stream transactions
   * @param from Data unique number from which reading will start
   * @param to Data unique number from which reading will stop
   * @return Queue of object which have storage type
   */
  override def get(streamName : String,
                   partition : Int,
                   transaction: UUID,
                   from: Int,
                   to: Int): scala.collection.mutable.Queue[Array[Byte]] = {
    val values : List[AnyRef] = List(streamName, new Integer(partition), transaction, new Integer(from), new Integer(to), new Integer(to-from+1))

    val statementWithBindings = selectStatement.bind(values:_*)

    logger.debug(s"start retrieving data for stream:{$streamName}, partition:{$partition}, from:{$from}, to:{$to}\n")
    val selected: util.List[Row] = session.execute(statementWithBindings).all()
    logger.debug(s"finished retrieving data for stream:{$streamName}, partition:{$partition}, from:{$from}, to:{$to}\n")

    val it = selected.iterator()
    val data = scala.collection.mutable.Queue[Array[Byte]]()

    while (it.hasNext){
      val obj = it.next().getObject("data").asInstanceOf[ByteBuffer].array()
      data.enqueue(obj)
    }

    data
  }

  /**
   * Close storage
   */
  def close() = {
    session.close()
    cluster.close()
  }

  /**
   * Validate that data storage created successfully
   */
  //@TODO implement validate and then delete deprecated annotation
  override def validate(): Boolean = ???

  /**
   * Initialize data storage
   */
  override def init(): Unit = {
    logger.info("start initializing CassandraStorage table\n")

    session.execute(s"CREATE TABLE data_queue ( " +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"seq int, " +
      s"data blob, " +
      s"PRIMARY KEY ((stream, partition), transaction, seq))")

    logger.info("finished initializing CassandraStorage table\n")
  }

  /**
   * Remove all data in data storage
   */
  override def truncate(): Unit = {
    logger.info("start truncating CassandraStorage data_queue table\n")

    session.execute("TRUNCATE data_queue")

    logger.info("finished truncating CassandraStorage data_queue table\n")
  }

  /**
   * Remove storage
   */
  override def remove(): Unit = {
    logger.info("start removing CassandraStorage data_queue table\n")

    session.execute("DROP TABLE data_queue")

    logger.info("finished removing CassandraStorage data_queue table\n")
  }

  /**
   * Checking closed or not this storage
   * @return Closed concrete storage or not
   */
  override def isClosed(): Boolean = session.isClosed && cluster.isClosed
}
