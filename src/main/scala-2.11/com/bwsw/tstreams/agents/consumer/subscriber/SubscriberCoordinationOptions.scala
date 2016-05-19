package com.bwsw.tstreams.agents.consumer.subscriber

import java.net.InetSocketAddress


case class SubscriberCoordinationOptions(agentAddress : String,
                                         prefix : String,
                                         zkHosts : List[InetSocketAddress],
                                         zkSessionTimeout : Int)