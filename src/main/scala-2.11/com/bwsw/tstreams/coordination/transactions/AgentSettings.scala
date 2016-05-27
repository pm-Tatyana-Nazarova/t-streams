package com.bwsw.tstreams.coordination.transactions

/**
 * Agent representation of every agent in [/producers/agents/{agent}]
 * @param id Agent id
 * @param penalty Penalty for agent (if he want to have low priority to be master)
 * @param priority Amount of partitions where this agent is master
 */
case class AgentSettings(id : String, var priority : Int, penalty : Int)


