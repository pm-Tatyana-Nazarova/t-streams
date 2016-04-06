package com.bwsw.tstreams.data.aerospike

import com.aerospike.client.Host
import com.aerospike.client.policy.{ClientPolicy, Policy, WritePolicy}

/**
 * @param namespace Aerospike namespace
 * @param hosts Aerospike hosts to connect
 * @param user User authentication to cluster
 * @param password Password authentication to cluster
 * @param failIfNotConnected If true throw exception if not connected
 * @param timeoutToOpenConnection Timeout to open connection  for the first time in milliseconds
 */
class AerospikeStorageOptions(val namespace : String,
                              val hosts : List[Host],
                              user : String = null,
                              password : String = null,
                              failIfNotConnected : Boolean = true,
                              timeoutToOpenConnection : Int = 1000) {
  if (namespace == null)
    throw new Exception("namespace can't be null")

  /**
   * Client policy
   */
  val policy = new ClientPolicy()
  policy.failIfNotConnected = failIfNotConnected
  policy.timeout = timeoutToOpenConnection
  if (user != null)
    policy.user = user
  if (password != null)
    policy.password = password

  /**
   * Write policy
   */
  val writePolicy = new WritePolicy()

  /**
   * Read policy
   */
  val readPolicy: Policy = new com.aerospike.client.policy.Policy
}
