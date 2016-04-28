package com.bwsw.tstreams_benchmarks.config_classes

case class ConfigLimitedByTime(InitMetadata: Boolean,
                               Cassandra: Map[String, Any],
                               Aerospike: Map[String, Any],
                               Redis: Map[String, Any],
                               DataStorageType: String,
                               InsertType: Map[String, Any],
                               ProducersNumber: Int,
                               ConsumersNumber: Int,
                               RecordByteSize: Int,
                               RecordsPerTransaction: Int,
                               TransactionsNumber: Int,
                               TransactionsPerGroup: Int,
                               OverallTime: Int,
                               StatisticsInterval: Int,
                               TTL: Int)
