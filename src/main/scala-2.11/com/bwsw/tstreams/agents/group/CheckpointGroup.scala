package com.bwsw.tstreams.agents.group

/**
 * Base class to creating agent group
 */
class CheckpointGroup() {
  /**
   * Group of agents (producers/consumer)
   */
  private var agents = scala.collection.mutable.Map[String,Agent]()

  /**
   * Validate that all agents has the same metadata storage
   */
  private def validateAgents() = {
    var set = Set[String]()
    agents.map(x=>x._2.getMetadataRef().id).foreach(id => set += id)
    if (set.size != 1)
      throw new IllegalStateException("agents must use only one common metadata storage")
  }

  /**
   * Add new agent in group
   * @param agent Agent ref
   * @param name Agent name
   */
  def add(name : String, agent : Agent) : Unit = {
    if (agents.contains(name))
      throw new IllegalArgumentException("agents with such name already exist")
    agents += ((name, agent))
    validateAgents()
  }

  /**
   * Remove agent from group
   * @param name Agent name
   */
  def remove(name : String) : Unit = {
    if (!agents.contains(name))
      throw new IllegalArgumentException("agents with such name not exist")
    agents.remove(name)
  }

  /**
   * Commit all agents state
   */
  def commit() : Unit = {
    val totalCommit: List[CommitInfo] = agents.map(x=>x._2.getCommitInfo()).reduceRight((l1,l2)=>l1 ++ l2)
    agents.head._2.getMetadataRef().groupCommitEntity.groupCommit(totalCommit)
  }
}
