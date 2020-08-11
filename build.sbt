name := "joyn-challenge"

version := "0.1"

scalaVersion := "2.11.7"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-2.4.0" + "_" + module.revision + "." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.0" % "provided"

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test