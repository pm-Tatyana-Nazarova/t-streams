package com.bwsw.tstreams.lockservice.impl

import com.bwsw.tstreams.lockservice.traits.ILockService
import com.twitter.common.zookeeper.{DistributedLockImpl, ZooKeeperClient}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
 * Locker for concrete path
 * @param zkClient ZooKeeper Client instance
 * @param path Path for lock
 * @param timeoutInSeconds Zk session timeout
 */

class ZkLockService(zkClient : ZooKeeperClient, path : String, timeoutInSeconds : Int) extends ILockService{

  /**
   * ZKLocker logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Instance of distributed lock impl based on zk
   */
  private val distributedLock = new DistributedLockImpl(zkClient, path)

  /**
   * If thread lock already locked zk path it will wait until it will become unlocked
   * else lock zk path
   */
  def lock() = {
    logger.info(s"lock path : {$path}")
    distributedLock.lock()
  }

  /**
   * Unlock zk path (if path not locked exception will be thrown)
   */
  def unlock() = {
    logger.info(s"unlock path : {$path}")
    distributedLock.unlock()
  }
}
