name := "serverActor"

version := "2.6"

scalaVersion := "2.11.3"

//assemblyJarName in assembly := "ServerActor-assembly-2.0.jar"


//libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.5.0"
//libraryDependencies += "com.typesafe.akka" % "akka-remote_2.11" % "2.5.0"
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0" //% "provided"
//libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0" //% "provided"


libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.0"
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0" //% "provided"

// https://mvnrepository.com/artifact/com.typesafe.akka/akka-protobuf_2.11
//libraryDependencies += "com.typesafe.akka" % "akka-protobuf_2.11" % "2.5.0"

//libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.1"


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.first
  case PathList("org", "aopalliance", xs @ _*)         => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith "axiom.xml" => MergeStrategy.filterDistinctLines
  case PathList(ps@_*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith "ILoggerFactory.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}