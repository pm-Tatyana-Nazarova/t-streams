package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTransactionStatus
import ProducerTransactionStatus._
import com.bwsw.tstreams.coordination.subscribe.messages.ProducerTransactionStatus

/**
 * Buffer for maintain consumed transactions in memory
 */
class TransactionsBuffer() {
  private val map : SortedExpiringMap[UUID, (ProducerTransactionStatus, Long)] =
    new SortedExpiringMap(new UUIDComparator, new SubscriberExpirationPolicy)

  def update(txnUuid : UUID, status: ProducerTransactionStatus, ttl : Int) : Unit = {
    if (!map.exist(txnUuid) && status == ProducerTransactionStatus.updated){
      return
    }
    if (map.exist(txnUuid)){
      if (map.get(txnUuid)._1 == ProducerTransactionStatus.closed) {
        return
      }
    }
    status match {
      case ProducerTransactionStatus.opened =>
        map.put(txnUuid, (status, ttl))

      case ProducerTransactionStatus.closed =>
        map.put(txnUuid, (status, -1)) //just ignore ttl because transaction is closed

      case ProducerTransactionStatus.cancelled =>
        map.remove(txnUuid)
    }
  }

  def getIterator() = map.entrySetIterator()
}
