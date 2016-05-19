package com.bwsw.tstreams.interaction.subscribe.messages

import java.util.UUID

import com.bwsw.tstreams.interaction.subscribe.messages.ProducerTransactionStatus.ProducerTransactionStatus

case class ProducerTopicMessage(txnUuid : UUID, ttl : Int, status : ProducerTransactionStatus)

