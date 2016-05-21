package com.bwsw.tstreams.coordination.subscribe.messages

import java.util.UUID

import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTransactionStatus.ProducerTransactionStatus

/**
 * Messages which is published by producer on every transaction update
 * @param txnUuid Transaction uuid
 * @param ttl Time of transaction expiration in seconds
 * @param status Transaction status
 */
case class ProducerTopicMessage(txnUuid : UUID, ttl : Int, status : ProducerTransactionStatus)

