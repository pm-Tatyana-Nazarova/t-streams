package com.bwsw.tstreams.txnqueue

import com.typesafe.scalalogging.Logger
import net.openhft.chronicle.queue.{ExcerptAppender, ChronicleQueueBuilder, ExcerptTailer}
import java.util.UUID
import java.util.concurrent.locks._
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue
import org.slf4j.LoggerFactory

/**
 * Queue for basic consumer with subscribe to maintain incoming and already existing messages
 * @param basePath base path for queues
 * @param separator element which used to indicate when start to read from second queue(if null second queue will be always used)
 */
class PersistentTransactionQueue (private val basePath : String,
                                  private val separator : UUID) {

    private var fromQ1 = !(separator == null)

    /**
     * Transaction queue logger for logging
     */
    private val logger = Logger(LoggerFactory.getLogger(this.getClass))

    private val q1: SingleChronicleQueue = ChronicleQueueBuilder.single(basePath + "/q1").build()
    private val q2: SingleChronicleQueue = ChronicleQueueBuilder.single(basePath + "/q2").build()

  /**
    * Chronicle stuff
    */
    private val appender1: ExcerptAppender = q1.createAppender()
    private val tailer1: ExcerptTailer = q1.createTailer()

    private val appender2: ExcerptAppender = q2.createAppender()
    private val tailer2: ExcerptTailer = q2.createTailer()

  /**
    * Queue blocking stuff
    */
    private val mutex = new ReentrantLock(true)
    private val cond = mutex.newCondition()

  /**
    * Put element in queue
    * @param value uuid to put
    */
    def put(value : UUID) {
        logger.debug(s"start put in queue uuid : $value\n")
        mutex.lock()

        if (separator == null)
          appender2.writeText(value.toString)
        else if (value.timestamp() <= separator.timestamp())
          appender1.writeText(value.toString)
        else
          appender2.writeText(value.toString)

        cond.signal()
        mutex.unlock()
        logger.debug(s"finished put in queue uuid : $value\n")
    }

  /**
    * Get element from queue
    */
    def get() : UUID = {
      logger.debug("start retrieving transaction from queue\n")
      mutex.lock()

      val t = {
        if (fromQ1)
          tailer1
        else
          tailer2
      }
      var data = t.readText()
      if(data == null) {
        cond.await()
        data = t.readText()
      }
      if (separator != null && data == separator.toString)
        fromQ1 = false
      val retrieved = UUID.fromString(data)

      mutex.unlock()

      logger.debug(s"finished retrieving transaction from queue : $retrieved\n")
      retrieved
    }

  /**
    * Close queue
    */
    def closeQueue() = {
        q1.close()
        q2.close()
    }
}