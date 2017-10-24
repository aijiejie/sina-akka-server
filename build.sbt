name := "serverActor"

version := "2.5"

scalaVersion := "2.11.8"

//assemblyJarName in assembly := "ServerActor-assembly-2.0.jar"

//libraryDependencies ++= Seq(
//  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
//  "com.typesafe.akka" % "akka-remote_2.11" % "2.3.9",
//  "log4j" % "log4j" % "1.2.17",
//  "org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided",
//  "org.apache.spark" %"spark-mllib_2.11" % "2.0.0" % "provided",
//  "com.twitter" % "chill-akka_2.11" % "0.8.4"
//)



/*akka依赖*/
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.5.0"
//libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.2.0"



libraryDependencies += "com.typesafe.akka" % "akka-remote_2.11" % "2.5.0"
//libraryDependencies += "com.typesafe.akka" % "akka-remote_2.10" % "2.2.0"


/*spark依赖*/
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0" //% "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0" //% "provided"


//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.0.0"  //% "provided"
//libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "2.0.0" //% "provided"

//libraryDependencies += "log4j" % "log4j" % "1.2.17"


//libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.4.1"
