package com.bwsw.tstreams.entities

import com.datastax.driver.core.Session

/**
 * Settings of stream in metadata storage
 * @param name Stream name
 * @param partitions Number of stream partitions
 * @param ttl Time in seconds of transaction expiration
 * @param description Some stream additional info
 */
case class StreamSettings(name : String, partitions : Int, ttl : Int, description : String)

/**
  * Stream entity for interaction with stream metadata
  * @param entityName name of certain table in C*
  */
class StreamEntity(entityName : String, session: Session){

  /**
   * Session prepared statement for stream creation
   */
  private val createStreamStatement = session
    .prepare(s"insert into $entityName (stream_name, partitions, ttl, description) values(?,?,?,?)")

  /**
   * Session prepared statement for stream deleting
   */
  private val deleteStreamStatement = session
    .prepare(s"delete from $entityName where stream_name=?")

  /**
   * Session prepared statement for stream settings retrieving
   */
  private val getStreamStatement = session
    .prepare(s"select * from $entityName where stream_name=? limit 1")

  /**
    * Create stream with parameters
    * @param name Stream name to use (unique id)
    * @param partitions Number of stream partitions
    * @param ttl Amount of expiration time of transaction
    * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
    * @return StreamSettings
    */
  def createStream(name : String,
                   partitions : Int,
                   ttl : Int,
                   description: String) : Unit = {
    if (isExist(name))
      throw new IllegalArgumentException("stream already exist")
    val values = List(name, new Integer(partitions), new Integer(ttl), description)
    val statementWithBindings = createStreamStatement.bind(values:_*)

    session.execute(statementWithBindings)
  }

  /**
   * Alternate stream with parameters
   * @param name Stream name to use (unique id)
   * @param partitions Number of stream partitions
   * @param ttl Amount of expiration time of transaction
   * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
   * @return StreamSettings
   */
  def alternateStream(name: String, partitions : Int, ttl : Int, description : String) : Unit = {
    if (!isExist(name))
      throw new IllegalArgumentException("stream to alternate does not exist")
    val values = List(name, new Integer(partitions), new Integer(ttl), description)
    val statementWithBindings = createStreamStatement.bind(values:_*)
    session.execute(statementWithBindings)

    StreamSettings(name,partitions,ttl,description)
  }

  /**
    * Deleting concrete stream
    * @param name Stream name to delete
    */
  def deleteStream(name : String): Unit = {
    if (!isExist(name))
      throw new IllegalArgumentException("stream not exist")
    val statementWithBindings = deleteStreamStatement.bind(name)
    session.execute(statementWithBindings)
  }

  /**
    * Checking that concrete stream exist
    * @param name Stream name to check if exists
    * @return Exist stream or not
    */
  def isExist(name : String) : Boolean = {
    val checkVal = getStream(name).isDefined
    checkVal
  }


  /**
    * Retrieving stream with concrete name
    * @param name Stream name to fetch from database
    * @return StreamSettings
    */
  def getStream(name : String) : Option[StreamSettings] = {
    val statementWithBindings = getStreamStatement.bind(name)
    val stream = session.execute(statementWithBindings).all()

    if (stream.isEmpty) None
    else {
      val value = stream.get(0)

      val name = value.getString("stream_name")
      val partitions = value.getInt("partitions")
      val description = value.getString("description")
      val ttl = value.getInt("ttl")

      Some(StreamSettings(name, partitions, ttl, description))
    }
  }
}
