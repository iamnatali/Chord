name := "Chord"

version := "0.1"

scalaVersion := "2.13.7"

val AkkaVersion = "2.6.18"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % AkkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % AkkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion
