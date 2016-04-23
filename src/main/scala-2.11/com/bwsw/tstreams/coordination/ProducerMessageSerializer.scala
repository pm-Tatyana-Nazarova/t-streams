package com.bwsw.tstreams.coordination

import com.bwsw.tstreams.common.JsonSerializer

object ProducerMessageSerializer {

  private val jsonSerializer = new JsonSerializer

  def serialize(msg : ProducerTopicMessage) : String = {
    jsonSerializer.serialize(msg)
  }

  def deserialize(msg : String) : ProducerTopicMessage = {
    jsonSerializer.deserialize[ProducerTopicMessage](msg)
  }
}
