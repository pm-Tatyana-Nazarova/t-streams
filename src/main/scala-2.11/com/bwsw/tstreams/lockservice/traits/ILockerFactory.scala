package com.bwsw.tstreams.lockservice.traits

/**
 * Basic trait for locker
 */
trait ILockerFactory {
  /**
   * Create locker with specific name
   * @param name
   */
  def createLocker(name : String) : Unit = ()

  /**
   * Get Locker with specific name
   * @param name
   * @return
   */
  def getLocker(name : String) : ILocker

  /**
   * Close all factory lockers
   */
  def closeFactory() : Unit

}
