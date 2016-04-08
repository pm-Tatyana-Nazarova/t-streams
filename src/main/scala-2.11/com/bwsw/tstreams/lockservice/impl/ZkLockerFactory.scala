package com.bwsw.tstreams.lockservice.impl

import java.net.InetSocketAddress
import com.bwsw.tstreams.lockservice.traits.ILockerFactory
import com.twitter.common.quantity.Amount
import com.twitter.common.zookeeper.ZooKeeperClient
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Service for exactly one access to some resource
 * @param zkServers Zk service params
 * @param rootPath Path to handle
 */
class ZkLockerFactory(zkServers : List[InetSocketAddress], rootPath : String, zkSessionTimeout : Int) extends ILockerFactory{
  /**
   * ZkLockerFactory logger for logging
   */
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
   * Max session timeout value
   */
  private val sessionTimeout = Amount.of(new Integer(zkSessionTimeout),com.twitter.common.quantity.Time.SECONDS)

  /**
   * Instances of all Lockers
   */
  private val instances = scala.collection.mutable.Map[String, ZkLocker]()

  /**
   * Zk client instance
   */
  private val zkClient: ZooKeeperClient = new ZooKeeperClient(sessionTimeout, zkServers.asJava)

  /**
   * Create Locker instance
   * @param additionalPath Some additional path for rootPath
   */
  def createLocker(additionalPath : String) : Unit = {
    logger.info(s"start creating locker with lockpath : {${rootPath+additionalPath}")
    val inst = new ZkLocker(zkClient, rootPath + additionalPath.toString, zkSessionTimeout)
    logger.info(s"finished creating locker with lockpath : {${rootPath+additionalPath}")
    instances += rootPath+additionalPath -> inst
  }

  /**
   * Get Locker instance
   * @param additionalPath Create locker instance for concrete additional path
   * @return Locker if it created, exception else
   */
  def getLocker(additionalPath : String) : ZkLocker = {
    logger.info(s"start retrieving locker with lockpath : {${rootPath+additionalPath}")
    if (!instances.contains(rootPath+additionalPath))
      throw new IllegalArgumentException("Requested Locker not exist")

    val inst = instances(rootPath+additionalPath)
    logger.info(s"finished retrieving locker with lockpath : {${rootPath+additionalPath}")
    inst
  }

  /**
   * Close all Locker instances
   */
  def closeFactory() = zkClient.close()
}