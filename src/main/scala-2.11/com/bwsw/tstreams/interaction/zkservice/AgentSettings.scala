package com.bwsw.tstreams.interaction.zkservice

/**
 * Agent representation of every agent in [/producers/agents/{agent}]
 * @param id Agent id
 * @param penalty Penalty for agent
 * @param priority Amount of partitions where this agent is master
 */
case class AgentSettings(id : String,var priority : Int, penalty : Int)


