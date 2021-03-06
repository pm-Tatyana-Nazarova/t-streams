package com.bwsw.tstreams.lockservice.impl

import com.bwsw.tstreams.lockservice.traits.ILocker
import com.typesafe.scalalogging.Logger
import org.redisson.core.RLock
import org.slf4j.LoggerFactory

/**
 * Locker using redis client
 */
class RedisLocker(locker : RLock) extends ILocker{

  /**
   * RedisLocker logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Lock this locker
   */
  override def lock(): Unit = {
    logger.info(s"lock path: {${locker.getName}}")
    locker.lock()
  }

  /**
   * Unlock this locker
   */
  override def unlock(): Unit = {
    logger.info(s"unlock path: {${locker.getName}}")
    locker.unlock()
  }
}
