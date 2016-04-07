name := "t-streams"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

//COMMON
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
  "com.gilt" % "gfc-timeuuid_2.11" % "0.0.6",
  "ch.qos.logback" % "logback-classic" % "1.1.6",
  "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0",
  "org.scalatest" % "scalatest_2.11" % "3.0.0-M15",
  "io.netty" % "netty-all" % "4.0.34.Final",
  "com.aerospike" % "aerospike-client" % "3.2.1"
)

libraryDependencies += ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0")
  .exclude("io.netty", "netty-common")
  .exclude("io.netty", "netty-codec")
  .exclude("io.netty", "netty-transport")
  .exclude("io.netty", "netty-buffer")
  .exclude("io.netty", "netty-handler")


//DISTRIBUTED LOCK DEPENDENCIES
resolvers += "twitter resolver" at "http://maven.twttr.com"
libraryDependencies += "com.twitter.common.zookeeper" % "lock" % "0.0.7"
libraryDependencies += ("org.redisson" % "redisson" % "2.2.10")
  .exclude("io.netty", "netty-common")
  .exclude("io.netty", "netty-codec")
  .exclude("io.netty", "netty-transport")
  .exclude("io.netty", "netty-buffer")
  .exclude("io.netty", "netty-handler")


//ASSEMBLY BUILD
assemblyJarName in assembly := "t-streams.jar"

assemblyMergeStrategy in assembly := {
  case PathList("io", "netty-all", xs @ _*) => MergeStrategy.first
  case PathList("com", "datastax", "cassandra", xs @ _*) => MergeStrategy.first
  case PathList("com", "typesafe", xs @ _*) => MergeStrategy.first
  case PathList("com", "gilt", xs @ _*) => MergeStrategy.first
  case PathList("ch", "qos", "logback", xs @ _*) => MergeStrategy.first
  case PathList("com", "typesafe", "scala-logging", xs @ _*) => MergeStrategy.first
  case PathList("org", "scalatest", xs @ _*) => MergeStrategy.first
  case PathList("com", "twitter", "common", "zookeeper", xs @ _*) => MergeStrategy.first
  case PathList("org", "redisson", xs @ _*) => MergeStrategy.first
  case PathList("com", "aerospike", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


//SBT TEST
parallelExecution in ThisBuild := false