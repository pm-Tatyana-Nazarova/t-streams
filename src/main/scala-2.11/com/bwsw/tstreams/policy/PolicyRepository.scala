package com.bwsw.tstreams.policy

import com.bwsw.tstreams.streams.BasicStream

/**
 * Repo for creating some defined policy
 */
object PolicyRepository {
  /**
   *
   * @param stream Stream instance
   * @param usedPartitions Policy partitions to use
   * @return RoundRobinPolicy instance
   */
  def getRoundRobinPolicy(stream : BasicStream[_], usedPartitions : List[Int]) : RoundRobinPolicy =
    new RoundRobinPolicy(stream, usedPartitions)
}