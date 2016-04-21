package com.bwsw.tstreams.agents.group

import com.bwsw.tstreams.metadata.MetadataStorage


/**
 * Base class to creating agent group
 * @param metadataStorage Metadata storage of all agents(every agent use this metadataStorage)
 */
class AgentsGroup(metadataStorage: MetadataStorage) {
  /**
   * Group of agents (producers/consumer)
   */
  private var agents = scala.collection.mutable.Map[String,Agent]()

  /**
   * Add new agent in group
   * @param agent Agent ref
   * @param name Agent name
   */
  def add(name : String, agent : Agent) : Unit = {
    if (agents.contains(name))
      throw new IllegalArgumentException("agents with such name already exist")
    agents += ((name, agent))
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
   * Commit all agent state
   */
  def commit() : Unit = {
    val totalCommit: List[CommitInfo] = agents.map(x=>x._2.getCommitInfo()).reduceRight((l1,l2)=>l1 ++ l2)
    metadataStorage.groupCommitEntity.groupCommit(totalCommit)
  }
}
