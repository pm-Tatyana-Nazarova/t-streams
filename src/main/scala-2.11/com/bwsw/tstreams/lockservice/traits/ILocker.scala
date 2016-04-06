package com.bwsw.tstreams.lockservice.traits

/**
 * Basic Locker trait
 */
trait ILocker {
  /**
   * Lock this locker
   */
  def lock() : Unit

  /**
   * Unlock this locker
   */
  def unlock() : Unit
}
