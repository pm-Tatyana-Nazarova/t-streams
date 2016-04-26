package com.bwsw.tstreams.coordination

import org.redisson.RedissonClient
import org.redisson.core.{RTopic, RLock}


/**
 * Redisson client wrapper
 * @param prefix Common prefix for all RedissonClient entities
 * @param client Redisson client
 */
class Coordinator(prefix : String,
                  client : RedissonClient){

  /**
   * Unique id for concrete coordinator
   */
  val id = java.util.UUID.randomUUID().toString

  /**
   * Creating lock on concrete prefix+path
   * @param path Lock path
   * @return Redisson RLock instance
   */
  def getLock(path : String): RLock = client.getLock(prefix + "/" + path)

  /**
   * Creating topic on concrete prefix+name
   * @param name Topic name
   * @tparam T Message type
   * @return RTopic instance
   */
  def getTopic[T](name : String): RTopic[T] = client.getTopic[T](prefix + "/" + name)
}

