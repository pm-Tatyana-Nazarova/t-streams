package com.bwsw.tstreams.coordination

import org.redisson.RedissonClient
import org.redisson.core.RLock


/**
 * Redisson client wrapper
 * @param prefix Common prefix for all RedissonClient entities
 * @param client Redisson client
 */
class Coordinator(prefix : String,
                  client : RedissonClient){

  /**
   * Creating lock on concrete prefix+path
   * @param path Lock path
   * @return Redisson RLock instance
   */
  def getLock(path : String): RLock = client.getLock(prefix + "/" + path)
}