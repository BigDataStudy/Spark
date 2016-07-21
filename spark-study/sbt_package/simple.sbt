name := "demo project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

