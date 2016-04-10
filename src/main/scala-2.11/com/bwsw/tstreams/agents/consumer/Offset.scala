package com.bwsw.tstreams.agents.consumer

import java.util.Date

/**
 * Basic trait for all offsets
 */
trait IOffset

/**
 * Oldest offset for data retrieving from the very beginning
 */
case object Oldest extends IOffset

/**
 * Newest offset for data retrieving from now
 */
case object Newest extends IOffset

/**
 * Offset for data retrieving from custom Date
 * @param startTime start offset value
 */
case class DateTime(startTime : Date) extends IOffset
