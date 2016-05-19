package com.bwsw.tstreams.coordination.subscribe.messages

import java.util.UUID

import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTransactionStatus.ProducerTransactionStatus

case class ProducerTopicMessage(txnUuid : UUID, ttl : Int, status : ProducerTransactionStatus)

