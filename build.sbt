name := "key-value-storage"

version := "1.0"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion
)