package com.bwsw.tstreams.entities.offsets

import java.util.{Date, UUID}

import com.gilt.timeuuid.TimeUuid

/**
 * Basic trait for all offsets
 */
trait IOffset{
  val value : UUID
}

/**
 * Oldest offset for data retrieving from the very beginning
 */
case object Oldest extends IOffset {
  override val value = TimeUuid(0)
}

/**
 * Newest offset for data retrieving from now
 */
case object Newest extends IOffset {
  //not implemented for it because it will be taken from different place
  override val value = ???
}

/**
 * Offset for data retrieving from custom Date
 * @param startTime start offset value
 */
case class DateTime(private val startTime : Date) extends IOffset {
  override val value = TimeUuid(startTime.getTime)
}
