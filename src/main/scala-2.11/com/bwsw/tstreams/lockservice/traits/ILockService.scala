package com.bwsw.tstreams.lockservice.traits

/**
 * Basic Locker trait
 */
trait ILockService {
  /**
   * Lock this locker
   */
  def lock() : Unit

  /**
   * Unlock this locker
   */
  def unlock() : Unit
}
