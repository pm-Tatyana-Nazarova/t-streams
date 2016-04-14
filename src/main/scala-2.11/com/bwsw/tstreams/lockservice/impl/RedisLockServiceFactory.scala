package com.bwsw.tstreams.lockservice.impl

import com.bwsw.tstreams.lockservice.traits.{ILockService, ILockServiceFactory}
import com.typesafe.scalalogging.Logger
import org.redisson.{RedissonClient, Redisson}
import org.redisson.core.RLock
import org.slf4j.LoggerFactory

/**
 * Redis Locker Factory
 * @param path Common path for all lockers
 * @param config Redisson Client config
 */
class RedisLockServiceFactory(path : String, config : org.redisson.Config) extends ILockServiceFactory{

  /**
   * RedisLockerFactory logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Reddison client for RLock instances creating
   */
  private val reddison: RedissonClient = Redisson.create(config)

  /**
   * Get Locker with specific name
   * @param name
   * @return
   */
  override def getLocker(name: String): ILockService = {
     logger.info(s"start retrieving redis locker with path: {$path$name}")
     val locker: RLock = reddison.getLock(path + name)
     val inst = new RedisLockService(locker)
     logger.info(s"finished retrieving redis locker with path: {$path$name}")
     inst
  }

  /**
   * Close all factory lockers
   */
  override def closeFactory(): Unit = reddison.shutdown()
}
