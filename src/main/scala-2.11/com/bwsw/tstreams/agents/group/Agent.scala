package com.bwsw.tstreams.agents.group


/**
 * Trait which can be implemented by any producer/consumer to apply group checkpoint 
 */
trait Agent {
  /**
   * Info to commit
   */
  protected def getCommitInfo() : List[CommitInfo]
}
