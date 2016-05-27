name := "t-streams"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

//COMMON
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.scalatest" % "scalatest_2.11" % "3.0.0-M15",
  "io.netty" % "netty-all" % "4.1.0.CR7",
  "com.aerospike" % "aerospike-client" % "3.2.1",
  "org.apache.commons" % "commons-collections4" % "4.1",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.3",
  "net.openhft" % "chronicle-queue" % "4.2.6",
  "org.slf4j" % "slf4j-simple" % "1.7.5" //remove
)

libraryDependencies += ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0")
  .exclude("io.netty", "netty-common")
  .exclude("io.netty", "netty-codec")
  .exclude("io.netty", "netty-transport")
  .exclude("io.netty", "netty-buffer")
  .exclude("io.netty", "netty-handler")


//COORDINATION
resolvers += "twitter resolver" at "http://maven.twttr.com"
libraryDependencies += "com.twitter.common.zookeeper" % "lock" % "0.0.40"


//ASSEMBLY STRATEGY
assemblyJarName in assembly := "t-streams.jar"

assemblyMergeStrategy in assembly := {
  case PathList("com","twitter","common","zookeeper", xs @ _*) => MergeStrategy.first
  case PathList("io", "netty", xs @ _*) => MergeStrategy.first
  case PathList("com", "datastax", "cassandra", xs @ _*) => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
  case PathList("org", "scalatest", xs @ _*) => MergeStrategy.first
  case PathList("org", "redisson", xs @ _*) => MergeStrategy.first
  case PathList("com", "aerospike", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("com", "fasterxml","jackson","module", xs @ _*) => MergeStrategy.first
  case PathList("net", "openhft", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}



//TESTS
parallelExecution in ThisBuild := false