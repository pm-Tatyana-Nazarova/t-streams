package com.bwsw.tstreams.newcoordination.subscribe.messages

import java.util.UUID

import com.bwsw.tstreams.newcoordination.subscribe.messages.ProducerTransactionStatus.ProducerTransactionStatus

case class ProducerTopicMessage(txnUuid : UUID, ttl : Int, status : ProducerTransactionStatus)

