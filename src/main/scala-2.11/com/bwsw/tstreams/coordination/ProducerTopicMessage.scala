package com.bwsw.tstreams.coordination

import java.util.UUID
import com.bwsw.tstreams.coordination.ProducerTransactionStatus.ProducerTransactionStatus

case class ProducerTopicMessage(txnUuid : UUID, ttl : Int, status : ProducerTransactionStatus)
