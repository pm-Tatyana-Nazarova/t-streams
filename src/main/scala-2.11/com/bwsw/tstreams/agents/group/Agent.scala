package com.bwsw.tstreams.agents.group


/**
 * Trait which can be implemented by any producer/consumer to apply group checkpoint 
 */
trait Agent {
  /**
   * Info to commit
   */
  def getCommitInfo() : List[CommitInfo]
}
